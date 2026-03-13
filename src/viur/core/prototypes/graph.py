import logging
import typing as t

from viur.core import current, db, errors, utils
from viur.core.bones import BooleanBone, KeyBone, NumericBone, SelectBone, SortIndexBone, StringBone
from viur.core.cache import flushCache
from viur.core.decorators import exposed, force_post, force_ssl, skey
from viur.core.skeleton import Skeleton, SkeletonInstance

from .list import List


EdgeDirection = t.Literal["in", "out", "both"]
NodeKind = t.Literal["state", "decision", "action", "start", "end"]
EdgeKind = t.Literal["transition", "condition_true", "condition_false", "error"]


class GraphNodeSkel(Skeleton):
    kind = SelectBone(
        descr="Node Kind",
        defaultValue="state",
        values={
            "state": "State",
            "decision": "Decision",
            "action": "Action",
            "start": "Start",
            "end": "End",
        },
    )
    graphDirected = BooleanBone(
        descr="Graph Directed",
        defaultValue=True,
        visible=False,
    )
    x = NumericBone(
        descr="Position X",
        precision=8,
        defaultValue=0.0,
        visible=False,
    )
    y = NumericBone(
        descr="Position Y",
        precision=8,
        defaultValue=0.0,
        visible=False,
    )
    parentrepo = KeyBone(
        descr="BaseRepo",
        visible=False,
    )
    sortindex = SortIndexBone(
        visible=False,
    )


class GraphEdgeSkel(Skeleton):
    kind = SelectBone(
        descr="Edge Kind",
        defaultValue="transition",
        values={
            "transition": "Transition",
            "condition_true": "Condition True",
            "condition_false": "Condition False",
            "error": "Error",
        },
    )
    source = KeyBone(
        descr="Source",
        visible=False,
        required=True,
    )
    target = KeyBone(
        descr="Target",
        visible=False,
        required=True,
    )
    directed = BooleanBone(
        descr="Directed",
        defaultValue=True,
        visible=True,
    )
    sourceHandle = StringBone(
        descr="Source Handle",
        visible=False,
        required=False,
    )
    targetHandle = StringBone(
        descr="Target Handle",
        visible=False,
        required=False,
    )
    parentrepo = KeyBone(
        descr="BaseRepo",
        visible=False,
    )


class Graph(List):
    """
    Graph module prototype with cyclic edges.

    Nodes are managed as list entries and edges as dedicated edge entities.
    """

    handler = "graph"
    accessRights = ("add", "edit", "view", "delete", "manage")

    nodeSkelCls = None
    edgeSkelCls = None

    def __init__(self, moduleName, modulePath, *args, **kwargs):
        assert self.nodeSkelCls, f"Need to specify nodeSkelCls for {self.__class__.__name__!r}"
        assert self.edgeSkelCls, f"Need to specify edgeSkelCls for {self.__class__.__name__!r}"
        super().__init__(moduleName, modulePath, *args, **kwargs)

    def _resolveSkelCls(self, *args, **kwargs) -> t.Type[Skeleton]:
        return self.nodeSkelCls

    def edgeSkel(self) -> SkeletonInstance:
        return self.edgeSkelCls()

    def ensureOwnModuleRootNode(self) -> db.Entity:
        """
        Ensures that one default root node exists.
        """
        key = "rep_module_repo"
        kind_name = self.viewSkel().kindName
        return db.GetOrInsert(db.Key(kind_name, key), creationdate=utils.utcNow(), rootNode=1, graphDirected=True)

    def getAvailableRootNodes(self, *args, **kwargs) -> list[dict[t.Literal["name", "key"], str]]:
        """
        Default function for providing a list of root node items.
        """
        return []

    @staticmethod
    def _normalize_direction(direction: t.Any) -> EdgeDirection:
        direction = (direction or "both")
        if isinstance(direction, str):
            direction = direction.lower()
        if direction not in ("in", "out", "both"):
            raise errors.NotAcceptable("direction must be one of: in, out, both")
        return direction

    @staticmethod
    def _as_bool(value: t.Any, default: bool = True) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        return utils.parse.bool(value)

    @staticmethod
    def _check_skel_type(skel_type: t.Any) -> t.Literal["node", "edge"]:
        skel_type = str(skel_type).lower()
        if skel_type not in ("node", "edge"):
            raise errors.NotAcceptable("Invalid skelType provided.")
        return t.cast(t.Literal["node", "edge"], skel_type)

    def _list_edge_query(self, query: db.Query) -> db.Query:
        query = self.listFilter(query)
        if not query or not query.queries:
            raise errors.Unauthorized()
        return query

    def _delete_edges_for_node_key(self, node_key: db.Key):
        edge_kind = self.edgeSkel().kindName
        edge_keys = set()

        for entry in db.Query(edge_kind).filter("source =", node_key).iter():
            edge_keys.add(entry.key)
        for entry in db.Query(edge_kind).filter("target =", node_key).iter():
            edge_keys.add(entry.key)

        for edge_key in edge_keys:
            edge_skel = self.edgeSkel()
            if edge_skel.fromDB(edge_key):
                edge_skel.delete()
        flushCache(kind=edge_kind)

    ## External exposed functions

    @exposed
    def listRootNodes(self, *args, **kwargs) -> t.Any:
        nodes = self.getAvailableRootNodes(*args, **kwargs) or []
        kind_name = self.viewSkel().kindName
        for item in nodes:
            if "directed" in item:
                continue
            root_key = item.get("key")
            if not root_key:
                item["directed"] = True
                continue
            try:
                key = db.keyHelper(root_key, kind_name)
                root_entity = db.Get(key)
                item["directed"] = bool(root_entity.get("graphDirected", True)) if root_entity else True
            except Exception:
                item["directed"] = True
        return self.render.listRootNodes(nodes)

    @exposed
    def list(
        self,
        skelType: str,
        nodeKey: db.Key | int | str = None,
        direction: EdgeDirection = "both",
        *args,
        **kwargs,
    ) -> t.Any:
        skel_type = self._check_skel_type(skelType)
        if "@rootNode" in kwargs and "parentrepo" not in kwargs:
            kwargs["parentrepo"] = kwargs["@rootNode"]

        if skel_type == "node":
            return super().list(*args, **kwargs)

        direction = self._normalize_direction(direction)

        if nodeKey is None:
            query = self._list_edge_query(self.edgeSkel().all().mergeExternalFilter(kwargs))
            self._apply_default_order(query)
            return self.render.list(query.fetch())

        node_key = db.keyHelper(nodeKey, self.viewSkel().kindName)

        def fetch_query(filter_name: str):
            q = self.edgeSkel().all().mergeExternalFilter(kwargs).filter(filter_name, node_key)
            q = self._list_edge_query(q)
            return q.fetch()

        if direction == "out":
            edges = {str(entry["key"]): entry for entry in fetch_query("source =")}
            for entry in self._list_edge_query(
                self.edgeSkel().all().mergeExternalFilter(kwargs).filter("target =", node_key).filter("directed =", False)
            ).fetch():
                edges[str(entry["key"])] = entry
            return self.render.list(list(edges.values()))

        if direction == "in":
            edges = {str(entry["key"]): entry for entry in fetch_query("target =")}
            for entry in self._list_edge_query(
                self.edgeSkel().all().mergeExternalFilter(kwargs).filter("source =", node_key).filter("directed =", False)
            ).fetch():
                edges[str(entry["key"])] = entry
            return self.render.list(list(edges.values()))

        edges = {}
        for entry in fetch_query("source ="):
            edges[str(entry["key"])] = entry
        for entry in fetch_query("target ="):
            edges[str(entry["key"])] = entry

        return self.render.list(list(edges.values()))

    @exposed
    def view(self, skelType: str, key: db.Key | int | str, *args, **kwargs) -> t.Any:
        skel_type = self._check_skel_type(skelType)

        if skel_type == "node":
            return super().view(key, *args, **kwargs)

        skel = self.edgeSkel()
        if not skel.fromDB(key):
            raise errors.NotFound()

        if not self.canView(skel):
            raise errors.Unauthorized()

        return self.render.view(skel)

    @exposed
    @force_ssl
    @skey(allow_empty=True)
    def add(self, skelType: str, node: db.Key | int | str = None, *, bounce: bool = False, **kwargs) -> t.Any:
        skel_type = self._check_skel_type(skelType)

        if skel_type == "node":
            parent_node = node or kwargs.get("node") or kwargs.get("parentrepo") or kwargs.get("@rootNode")
            if not parent_node:
                raise errors.NotAcceptable("Missing required parameter 'node'")

            parent_skel = self.viewSkel()
            if not parent_skel.fromDB(parent_node):
                raise errors.NotFound("The provided parent node could not be found.")
            if not (self.canAdd() and self.canView(parent_skel)):
                raise errors.Unauthorized()

            skel = self.addSkel()
            skel["parentrepo"] = parent_skel["key"]

            client_data = dict(kwargs)
            client_data.pop("node", None)
            client_data.pop("parentrepo", None)
            client_data.pop("@rootNode", None)
            client_data.pop("bounce", None)
            client_data.pop("skey", None)

            if (
                not client_data
                or not current.request.get().isPostRequest
                or not skel.fromClient(client_data, amend=bounce)
                or bounce
            ):
                return self.render.add(skel)

            self.onAdd(skel)
            skel.write()
            self.onAdded(skel)
            return self.render.addSuccess(skel)

        if not self.canAdd():
            raise errors.Unauthorized()

        edge = self.edgeSkel()
        source_skel = self.viewSkel()
        target_skel = self.viewSkel()

        if (
            not kwargs
            or not current.request.get().isPostRequest
            or not edge.fromClient(kwargs, amend=True)
            or bounce
        ):
            return self.render.add(edge)

        if not source_skel.fromDB(edge["source"]):
            raise errors.NotFound("Cannot find source node")
        if not target_skel.fromDB(edge["target"]):
            raise errors.NotFound("Cannot find target node")

        if not (self.canView(source_skel) and self.canView(target_skel)):
            raise errors.Unauthorized()

        root_repo_key = source_skel["parentrepo"]
        if "parentrepo" in kwargs and kwargs["parentrepo"]:
            try:
                root_repo_key = db.keyHelper(kwargs["parentrepo"], self.viewSkel().kindName)
            except Exception:
                pass

        root_directed = True
        if root_repo_key:
            root_repo_skel = self.viewSkel()
            if root_repo_skel.fromDB(root_repo_key):
                root_directed = bool(root_repo_skel["graphDirected"])

        edge["directed"] = root_directed
        if "parentrepo" in edge and "parentrepo" in source_skel:
            edge["parentrepo"] = source_skel["parentrepo"]

        self.onAdd(edge)
        edge.toDB()
        self.onAdded(edge)

        flushCache(kind=edge.kindName)
        return self.render.addSuccess(edge)

    @exposed
    @force_ssl
    @force_post
    @skey
    def delete(self, skelType: str, key: str, *args, **kwargs) -> t.Any:
        skel_type = self._check_skel_type(skelType)

        if skel_type == "node":
            skel = self.editSkel()
            if not skel.fromDB(key):
                raise errors.NotFound()

            if not self.canDelete(skel):
                raise errors.Unauthorized()

            self._delete_edges_for_node_key(skel["key"])
            self.onDelete(skel)
            skel.delete()
            self.onDeleted(skel)
            return self.render.deleteSuccess(skel)

        edge = self.edgeSkel()
        if not edge.fromDB(key):
            raise errors.NotFound()

        if not self.canDelete(edge):
            raise errors.Unauthorized()

        self.onDelete(edge)
        edge.delete()
        self.onDeleted(edge)
        flushCache(key=edge["key"])
        return self.render.deleteSuccess(edge)

    @exposed
    @force_ssl
    @force_post
    @skey
    def connect(
        self,
        source: db.Key | int | str,
        target: db.Key | int | str,
        directed: bool | str = True,
        sourceHandle: str | None = None,
        targetHandle: str | None = None,
        *args,
        **kwargs,
    ):
        source_skel = self.viewSkel()
        target_skel = self.viewSkel()

        if not source_skel.fromDB(source):
            raise errors.NotFound("Cannot find source node")
        if not target_skel.fromDB(target):
            raise errors.NotFound("Cannot find target node")

        if not self.canAdd():
            raise errors.Unauthorized()
        if not (self.canView(source_skel) and self.canView(target_skel)):
            raise errors.Unauthorized()

        edge = self.edgeSkel()
        edge["source"] = source_skel["key"]
        edge["target"] = target_skel["key"]
        root_repo_key = source_skel["parentrepo"]
        if "parentrepo" in kwargs and kwargs["parentrepo"]:
            try:
                root_repo_key = db.keyHelper(kwargs["parentrepo"], self.viewSkel().kindName)
            except Exception:
                pass

        root_directed = True
        if root_repo_key:
            root_repo_skel = self.viewSkel()
            if root_repo_skel.fromDB(root_repo_key):
                root_directed = bool(root_repo_skel["graphDirected"])

        edge["directed"] = root_directed
        edge["sourceHandle"] = sourceHandle if sourceHandle else ""
        edge["targetHandle"] = targetHandle if targetHandle else ""

        if "parentrepo" in edge and "parentrepo" in source_skel:
            edge["parentrepo"] = source_skel["parentrepo"]

        if not current.request.get().isPostRequest:
            return self.render.add(edge)

        client_data = dict(kwargs)
        client_data.pop("source", None)
        client_data.pop("target", None)
        client_data.pop("directed", None)
        client_data.pop("sourceHandle", None)
        client_data.pop("targetHandle", None)
        client_data.pop("bounce", None)
        client_data.pop("skey", None)

        if client_data and not edge.fromClient(client_data, amend=True):
            return self.render.add(edge)
        if utils.parse.bool(kwargs.get("bounce")):
            return self.render.add(edge)

        edge.toDB()
        flushCache(kind=edge.kindName)

        logging.info(f"""Edge added: {edge["key"]!r}""")
        if user := current.user.get():
            logging.info(f"""User: {user["name"]!r} ({user["key"]!r})""")

        return self.render.addSuccess(edge)

    @exposed
    @force_ssl
    @force_post
    @skey
    def disconnect(self, edgeKey: db.Key | int | str, *args, **kwargs):
        edge = self.edgeSkel()
        if not edge.fromDB(edgeKey):
            raise errors.NotFound()

        if not self.canDelete(edge):
            raise errors.Unauthorized()

        edge.delete()
        flushCache(key=edge["key"])
        return self.render.deleteSuccess(edge)

    @exposed
    @force_ssl
    @skey(allow_empty=True)
    def edit(
        self,
        skelType: str,
        key: db.Key | int | str,
        *,
        bounce: bool = False,
        **kwargs,
    ) -> t.Any:
        skel_type = self._check_skel_type(skelType)

        if skel_type == "node":
            return super().edit(key, bounce=bounce, **kwargs)

        edge = self.edgeSkel()
        if not edge.fromDB(key):
            raise errors.NotFound()

        if not self.canEdit(edge):
            raise errors.Unauthorized()

        if (
            not kwargs
            or not current.request.get().isPostRequest
            or not edge.fromClient(kwargs, amend=True)
            or bounce
        ):
            return self.render.edit(edge)

        self.onEdit(edge)
        edge.toDB()
        self.onEdited(edge)

        return self.render.editSuccess(edge)

Graph.admin = True
Graph.vi = True
