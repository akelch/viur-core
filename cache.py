# -*- coding: utf-8 -*-
from viur.server import db, utils, request, tasks, session
from viur.server.config import conf
from hashlib import sha512
from datetime import datetime, timedelta
import logging
from functools import wraps

"""
	This module provides a cache, allowing to serve
	whole queries from that cache. Unlike other caches
	implemented in ViUR, it caches the actual result
	(ie the html-output in most cases). This can also
	be used to cache the output of custom build functions.
	Admins can bypass this cache by sending the X-Viur-Disable-Cache http Header
	along with their requests.
"""

viurCacheName = "viur-cache"


def keyFromArgs(f, userSensitive, languageSensitive, evaluatedArgs, path, args, kwargs):
	"""
		Parses args and kwargs according to the information's given
		by evaluatedArgs and argsOrder. Returns an unique key for this
		combination of arguments. This key is guaranteed to be stable for
		subsequent calls with the same arguments and context( the current user)

		:param f: Callable which is inspected for its signature
			(we need to figure out what positional arguments map to which key argument)
		:type f: Callable
		:param userSensitive: Signals wherever the output of f depends on the current user.
			0 means independent of wherever the user is a guest or known, all will get the same content.
			1 means cache only for guests, no cache will be performed if the user is logged-in.
			2 means cache in two groups, one for guests and one for all users
			3 will cache the result of that function for each individual users separately.
		:type userSensitive: int
		:param evaluatedArgs: List of keyword-arguments having influence to the output generated by
			that function. This list *must* complete! Parameters not named here are ignored!
		:type evaluatedArgs: list
		:param path: Path to the function called but without parameters (ie. "/page/view")
		:type path: str
		:returns: The unique key derived
	"""
	res = {}
	argsOrder = list(f.__code__.co_varnames)[1: f.__code__.co_argcount]
	# Map default values in
	reversedArgsOrder = argsOrder[:: -1]
	for defaultValue in list(f.func_defaults or [])[:: -1]:
		res[reversedArgsOrder.pop(0)] = defaultValue
	del reversedArgsOrder
	# Map args in
	setArgs = []  # Store a list of args already set by *args
	for idx in range(0, min(len(args), len(argsOrder))):
		if argsOrder[idx] in evaluatedArgs:
			setArgs.append(argsOrder[idx])
			res[argsOrder[idx]] = args[idx]
	# Last, we map the kwargs in
	for k, v in kwargs.items():
		if k in evaluatedArgs:
			if k in setArgs:
				raise AssertionError("Got dupplicate arguments for %s" % k)
			res[k] = v
	if userSensitive:
		user = utils.getCurrentUser()
		if userSensitive == 1 and user:  # We dont cache requests for each user seperately
			return (None)
		elif userSensitive == 2:
			if user:
				res["__user"] = "__ISUSER"
			else:
				res["__user"] = None
		elif userSensitive == 3:
			if user:
				res["__user"] = user["key"]
			else:
				res["__user"] = None
	if languageSensitive:
		res["__lang"] = request.current.get().language
	if conf["viur.cacheEnvironmentKey"]:
		try:
			res["_cacheEnvironment"] = conf["viur.cacheEnvironmentKey"]()
		except RuntimeError:
			return None
	res["__path"] = path  # Different path might have different output (html,xml,..)
	try:
		appVersion = request.current.get().request.environ["CURRENT_VERSION_ID"].split('.')[0]
	except:
		appVersion = ""
		logging.error("Could not determine the current application id! Caching might produce unexpected results!")
	res["__appVersion"] = appVersion
	# Last check, that every parameter is satisfied:
	if not all([x in res.keys() for x in argsOrder]):
		# we have too few paramerts for this function; that wont work
		return None
	res = list(res.items())  # Flatn our dict to a list
	res.sort(key=lambda x: x[0])  # sort by keys
	mysha512 = sha512()
	mysha512.update(str(res).encode("UTF8"))
	return (mysha512.hexdigest())


def wrapCallable(f, urls, userSensitive, languageSensitive, evaluatedArgs, maxCacheTime):
	"""
		Does the actual work of wrapping a callable.
		Use the decorator enableCache instead of calling this directly.
	"""

	@wraps(f)
	def wrapF(self, *args, **kwargs):
		currentRequest = request.current.get()
		if conf["viur.disableCache"] or currentRequest.disableCache:
			# Caching disabled
			if conf["viur.disableCache"]:
				logging.debug("Caching is disabled by config")
			return (f(self, *args, **kwargs))
		# How many arguments are part of the way to the function called (and how many are just *args)
		offset = -len(currentRequest.args) or len(currentRequest.pathlist)
		path = "/" + "/".join(currentRequest.pathlist[: offset])
		if not path in urls:
			# This path (possibly a sub-render) should not be cached
			logging.debug("Not caching for %s" % path)
			return (f(self, *args, **kwargs))
		key = keyFromArgs(f, userSensitive, languageSensitive, evaluatedArgs, path, args, kwargs)
		if not key:
			# Someting is wrong (possibly the parameter-count)
			# Letz call f, but we knew already that this will clash
			return (f(self, *args, **kwargs))
		try:
			dbRes = db.Get(db.Key.from_path(viurCacheName, key))
		except db.EntityNotFoundError:
			dbRes = None
		if dbRes:
			if not maxCacheTime or \
					dbRes["creationtime"] > datetime.now() - timedelta(seconds=maxCacheTime):
				# We store it unlimited or the cache is fresh enough
				logging.debug("This request was served from cache.")
				currentRequest.response.headers['Content-Type'] = dbRes["content-type"].encode("UTF-8")
				return (dbRes["data"])
		# If we made it this far, the request wasnt cached or too old; we need to rebuild it
		res = f(self, *args, **kwargs)
		dbEntity = db.Entity(viurCacheName, name=key)
		dbEntity["data"] = res
		dbEntity["creationtime"] = datetime.now()
		dbEntity["path"] = path
		dbEntity["content-type"] = request.current.get().response.headers['Content-Type']
		dbEntity.set_unindexed_properties(["data", "content-type"])  # We can save 2 DB-Writs :)
		db.Put(dbEntity)
		logging.debug("This request was a cache-miss. Cache has been updated.")
		return (res)

	return wrapF


def enableCache(urls, userSensitive=0, languageSensitive=False, evaluatedArgs=[], maxCacheTime=None):
	"""
		Decorator to mark a function cacheable.
		Only functions decorated with enableCache are considered cacheable;
		ViUR will never ever cache the result of a user-defined function otherwise.
		Warning: It's not possible to cache the result of a function relying on reading/modifying
		the environment (ie. setting custom http-headers). The only exception is the content-type header.

		:param urls: A list of urls for this function, for which the cache should be enabled.
			A function can have several urls (eg. /page/view or /pdf/page/view), and it
			might should not be cached under all urls (eg. /admin/page/view).
		:type urls: list
		:param userSensitive: Signals wherever the output of f depends on the current user.
			0 means independent of wherever the user is a guest or known, all will get the same content.
			1 means cache only for guests, no cache will be performed if the user is logged-in.
			2 means cache in two groups, one for guests and one for all users
			3 will cache the result of that function for each individual users separately.
		:type userSensitive: int
		:param languageSensitive: If true, signals that the output of f might got translated.
			If true, the result of that function is cached separately for each language.
		:type languageSensitive: Bool
		:param evaluatedArgs: List of keyword-arguments having influence to the output generated by
			that function. This list *must* be complete! Parameters not named here are ignored!
			Warning: Double-check this list! F.e. if that function generates a list of entries and
			you miss the parameter "order" here, it would be impossible to sort the list.
			It would always have the ordering it had when the cache-entry was created.
		:type evaluatedArgs: list
		:param maxCacheTime: Specifies the maximum time an entry stays in the cache in seconds.
			Note: Its not erased from the db after that time, but it won't be served anymore.
			If None, the cache stays valid forever (until manually erased by calling flushCache.
		:type maxCacheTime: int or None

	"""
	assert not any([x.startswith("_") for x in evaluatedArgs]), "A evaluated Parameter cannot start with an underscore!"
	return lambda f: wrapCallable(f, urls, userSensitive, languageSensitive, evaluatedArgs, maxCacheTime)


@tasks.callDeferred
def flushCache(prefix="/*"):
	"""
		Flushes the cache. Its possible the flush only a part of the cache by specifying
		the path-prefix.

		:param prefix: Path or prefix that should be flushed.
		:type prefix: str

		Examples:
			- "/" would flush the main page (and only that),
			- "/*" everything from the cache, "/page/*" everything from the page-module (default render),
			- and "/page/view/*" only that specific subset of the page-module.
	"""
	items = db.Query(viurCacheName).filter("path =", prefix.rstrip("*")).iter(keysOnly=True)
	for item in items:
		db.Delete(item)
	if prefix.endswith("*"):
		items = db.Query(viurCacheName).filter("path >", prefix.rstrip("*")).filter("path <", prefix.rstrip(
			"*") + u"\ufffd").iter(keysOnly=True)
		for item in items:
			db.Delete(item)
	logging.debug("Flushing cache succeeded. Everything matching \"%s\" is gone." % prefix)


__all__ = ["enableCache", "flushCache"]
