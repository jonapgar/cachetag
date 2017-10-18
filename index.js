var map={};
var hash = require('./hash');
var _  = require('lodash');

var fs = require('fs');
var path = require('path')
var Promise = require('promise');

var zlib = require('zlib');
var gzip = zlib.gzip;
var gunzip = zlib.gunzip;
var loose = require('./setloose.js')({resolution:1000,cleanInterval:60000});

var debug = require('debug');

var _log = global._log || debug('cachetag:log');
var _notice = global._notice || debug('cachetag:notice');
var _warning = global._warning || debug('cachetag:warning');
var _error = global._error || debug('cachetag:error');


function logerr(err,results){
	if (err)
		return _error(err)
}
function cache(key,timeoutSetting,autorefresh,persist,version) {
	key = hash(key);
	if (key in map)
		return map[key]
	
	if (autorefresh===undefined) {
		autorefresh=true;
	}
	if (timeoutSetting===undefined) {
		timeoutSetting = 5*60 //5 mins
	}
	_notice('creating cache for %s with timeoutSetting %s and autorefresh %s',key,timeoutSetting,autorefresh);
	var c = map[key] = new Cache(key,timeoutSetting,autorefresh,persist,version);
	return c;
}
function Cache(key,timeoutSetting,autorefresh,persist,version){
	if (!version)
		version = '';
	key = hash(key);

	var map =this.map = {};
	this.timeoutSetting = timeoutSetting;
	this.autorefresh = autorefresh;
	this.key = key;

	var self=this;

	

	// persist = false;


	if (persist) {
		_notice('persisting cache %s',key);
		this.persist = _.clone(cache('persist').get(_.pick(persist,['prefix','db','host']),function(){
			_log('redis options %o',persist);

			var r = require('redis').createClient(_.assign(persist,{
				// parser:'hiredis',
				detect_buffers:true, //this must be true for gzip.
			}));
			
			return {
				redis:r,
				fetch:function(c,list){
					_log('redis fetch %s',c.key);
					var key = c.key + version
					if (!list) {
						r.get(key,function(err,list){
							if (err)
								return logerr(err);
							try {
								list = JSON.parse(list);
							} catch (e) {
								return logerr(e)
							}
							_log('redis found list %s',persist.prefix + key,list)
							if (list && list.length) {
								_.defer(function(){
									next(list); 	
								})
							}
								
						});
					}
					function next(list){
						_log('redis next %s',c.key);
						var buffers = _.map(list,function(v){return new Buffer(v)});
						r.mget(buffers,function(err,results){
							if (err)
								return logerr(err);

							_log('redis found %d of %d results for %s',results.length,list.length,persist.prefix + key);
							_.each(results,function(v,i){
								if (!v)
									return;
								
								gunzip(v,function(err, v){
									
									if(!_.isObject(v))
										v=v.toString();
									if (err)
										return logerr(err)
									
									try {
										v = JSON.parse(v);
										_log('redis found %s => %s tagged %o',persist.prefix + key,list[i],v.tags);

									} catch (e) {
										return logerr(e);
									}
									
									var k = v.key;
									if ('resolved' in v) {
										v.data=new Promise(function(resolve,reject){
											resolve(v.resolved)
										});
									} 
									if ('data' in v) 
										c.update(v.timestamp,k,v.data,c.persist.smudge,v.tags)

								})

							})
						})
					}
				},
				syncOne:function(c,v,k,cb){

					if (!_.isFunction(cb)) {
						cb = function(err){
							logerr(err)
						}
					}
					var key = c.key + version
					var ttl = timeoutSetting || 60*60*24*7;
					if (v.isPromise) {
						if ('resolved' in v)
							v = _.pick(v,['resolved','timestamp','key','tags'])
						else
							cb(new Error('could not sync ' + k + '. promise not resolved'))
					} else {
						v = _.pick(v,['data','timestamp','key','tags'])
					}
					
					if (_.isFunction(c.persist.clean)){
						var d = v.isPromise ? 'resolved':'data'
						v[d] = c.persist.clean.call(c,v[d],k);
					}
					try {
						v = JSON.stringify(v,null,'\t')
					} catch(e) {
						_error('%s=>%s could not be stringified',key,k)
						return cb(e)
					}
					var h = hash(key,k);
					var start = Date.now();
					_log('redis BEGIN setting %s => %s = %s',persist.prefix + key,persist.prefix +h,v.substring(0,32) + '...');
					//defer this call, to allow setting list to happen FIRST in sync. 
					_.defer(function(){
						gzip(v,function(err,v){
							if (err) {
								
								return cb(err)
							}
							r.set(h,v,'EX',ttl,function(err){
								var end = Date.now();
								if (err) {
									
									return cb(err)
									
								}
									
								_log('redis FINISH setting %s => %s = %s',persist.prefix +key,persist.prefix +h);
								
								cb(null)
							})		
						})
						
						c.persist.syncList(c,_.keys(c.map));
					})
					return h;
				},
				syncList:_.throttle(function(c,keys,cb){
					var key = c.key + version
					
					var ttl = timeoutSetting || 60*60*24*7;

					// var list = _.map(c.map,_.partial(c.persist.syncOne,c)) || [];

					var list = _.map(keys,function(k){
						return hash(key,k);
					}) || [];
					
					_log('redis setting list %s => %o',persist.prefix +key,list)
					r.set(key,JSON.stringify(list),'EX',ttl,cb)
				},10000,{leading:true,trailing:true}),
			}
		}));
		this.persist.clean = persist.clean;
		this.persist.smudge = persist.smudge;
		this.persist.fetch(this)
	}
	
}

function Val(cache,key,data,destroy,timeoutSetting,autorefresh) {
	var v = this;
	v.data = data;
	v.tags=[];
	v.cache = cache;
	v.key = key;
	v.destroy = v.destroy;
	v.timestamp = Date.now()
	makeTimeout(v,timeoutSetting,function(){
		cache.unset(key)
	});
	v.autorefresh = autorefresh;
}

// var i = 0;
// function HashMap(){
// 	return this;
// }
// HashMap.prototype.init = function(){
// 	if (this.initialized)
// 		return;
	
// 	var self=this;
// 	this.cache = new Cache('hm_'+i,5,false)
// 	this.timeout =setTimeout(function(){
// 		self.initialize=false;
// 		self.cache.clear();
// 	},5000)
// 	i++;
// 	this.initialized=true;
// }
// HashMap.prototype.get=function(key){
// 	this.init();
// 	return this.cache.get(key)
// }
// HashMap.prototype.set=function(key,value){
// 	this.init();
// 	this.cache.set(key,value);
// 	return this;
// }
// HashMap.prototype.has= function(key){
// 	this.init();
// 	return this.cache.has(key)
// }

// cache.HashMap = HashMap;

var valProt = Val.prototype;

valProt.unset = function(){
	clearTimeout(this.timeout);
	if (this.destroy) {
		_log('destroying %s=>%s',this.cache.key,this.key);
		this.destroy.call(this.data)
	}
}

function makeTimeout(v,n,f) {
	clearTimeout(v.timeout)
	if (n && _.isFunction(f))
		v.timeout = loose.setLooseTimeout(f,n*1000)
}

cache.hash = hash;


cache.teardown = function(){
	_.each(map,function(v,key){
		if (v.persist) {
			clearInterval(v.persist.interval)
			v.persist.syncList(v,_.keys(v.map),function(){
				v.persist.redis.unref();
			 	v.persist.redis.quit();
			});
		}
		v.clear();
	},this);
	map={};
}
cache.destroy= cache.unset = function(key){
	key=hash(key);
	if (key in map) {
		map[key].clear();
		delete map[key];
	}
}

cache.clear = function(key){
	map[key].clear();
}

cache.Cache = Cache;

var prot = Cache.prototype;

prot.hash = cache.hash;

prot.has = function(key){
	key = hash(key)
	
	return key in this.map;
}
prot.clear = function(){
	_log('clearing %s',this.key);
	_.each(this.map,function(v,k){
		this.unset(k);
	},this)
}
prot.all = function(){
	return _.mapValues(this.map,function(v,k){
		return this.get(k)
	},this)
}

prot.prep = function(key,generate,destroy) {
	var seed = key;
	key = hash(key);
	var v = this.map[key];

	if (v) return v.spawn || function(){return v.data};


	var cache = this;
	
	this.set(key,function(){
		//will trigger spawn.
		cache.get(key)
	},destroy);


	v = this.map[key];
	v.spawn = _.once(function(){
		return generate(seed);
	})
	return v.spawn;
}

prot.select = function(key,generate,destroy){
	var seed = key;
	key=hash(key);
	if (!(key in this.map)){
		_log('could not find %s=>%s',this.key,key);
		if (generate){
			_notice('generating %s=>%s',this.key,key);
			var v = _.isFunction(generate) ? generate(seed):generate;
			return this.set(key,v,destroy);
			
		}

		return undefined
	}

	var v = this.map[key];
	if (v.spawn) {
		var spawn = v.spawn;
		_log('spawning %s=>%s',this.key,key);
		delete v.spawn;
		v.data = spawn();

	}
	if (v.autorefresh && this.timeoutSetting) {
		makeTimeout(v,this.timeoutSetting);
	}
	_log('found %s=>%s with tags %o',this.key,key,v.tags);
	
	return v;
}

prot.get = function(key,generate,destroy){
	var v = this.select.apply(this,arguments);
	return v ? v.data:undefined;
}

prot.getTags = function(key,generate,destroy){
	var v = this.select.apply(this,arguments);
	return v ? (v.tags || []):undefined;
}

prot.tag = function(key) {
	key = hash(key);
	var tags = _.toArray(arguments).slice(1);
	if (!(key in this.map)){
		_error('CANNOT TAG %s=>%s with %o',this.key,key,tags);
		throw new Error('cannot tag nonexistant cache entry')
	}
	var v = this.map[key];
	
	v.tags.push.apply(v.tags,tags);
	_log('tagging %s=>%s with %o',this.key,key,tags);
	return v.data;
}

prot.findByTags=function(){
	var tags = _.toArray(arguments);
	var v= _.find(this.map,function(v){
		return _.any(tags,function(t){
			return v.tags.indexOf(t)>-1
		})
	})
	return v ? v.data:undefined
}

prot.invalidateByTags=function(){
	var tags = _.toArray(arguments);
	var keys = _.transform(this.map,function(m,v,k){

		if (_.any(tags,function(t){
			return v.tags.indexOf(t)>-1
		}))
			m.push(k)
	},[])
	if (_.isEmpty(keys)) {
		_notice('nothing to invalidate on %s for tags %o',this.key,tags);
		return;
	}
	_.each(keys,function(k){
		_notice('unsetting %s=>%s from tags %o',this.key,k,tags);
		this.unset(k);
	},this)
}

prot.set = function(key,data,destroy,persist,tags){
	key = hash(key);
	_log('setting %s=>%s',this.key,key);
	
	
	timeoutSetting = this.timeoutSetting;

	autorefresh = this.autorefresh;

	var v = this.map[key];
	if (data && _.isFunction(data.then)) {
		var isPromise = true;
		
	}


	if (v) {
		if (!isPromise) {
			delete v.resolved;
		}
		delete v.spawn;
		v.data = data;
		v.destroy = destroy || v.destroy;
		v.autorefresh = autorefresh;
		v.timestamp = Date.now();
		makeTimeout(v,timeoutSetting,function(){
			v.cache.unset(key)
		});
	}  else {
		v = this.map[key] = new Val(this,key,data,destroy,timeoutSetting,autorefresh)
	}
	

	if (this.persist && persist!==false) {
		var self=this;

		v.doSync = function(){
			delete this.doSync;
			self.persist.syncOne(self,v,key,function(err){
				if (err)
					return logerr(err)
				process.send && process.send({event:{event:'cache',cacheKey:self.key,key:key}});
			});	
		}
	} else {
		delete v.doSync;
	}

	if (isPromise) {
		data.then(function(resolved){
			v.resolved = resolved;
			v.doSync && v.doSync();
		},function(rejected){
			delete v.doSync
		}).catch(logerr);
	} else {
		v.doSync && v.doSync();
	}
	v.isPromise = !!isPromise;
	v.tags = tags || [];
	return v;
}

prot.update = function(timestamp, key,value,smudge,tags){
	//only update the key if the new timestamp is later than the one we have
	if (key in this.map){
		var v = this.map[key];
		if (timestamp <= v.timestamp) {
			_notice('not updating old value %s => %s',this.key,key)
			return v
		}
		_notice('updating old value %s => %s',this.key,key)	

	} 
	var args = _.slice(arguments,1);
	if (smudge)
		value = smudge.call(this,value,key);
	var v =  this.set(key,value,null,false,tags);
	
	
	return v;
}



prot.unset=function(key) {
	key = hash(key);
	if (!(key in this.map))
		return undefined;
	var v = this.map[key];
	v.unset();
	_log('removing %s=>%s',this.key,key);
	delete this.map[key];
	
	return undefined
}

// _.each(prot,function(v,k){
// 	prot[k] = function(){
// 		var key = this.key;
// 		_log('accessing cache %s',this.key);
// 		makeTimeout(this,this.timeoutSetting,function(){
// 			cache.unset(key)
// 		});
// 		v.apply(this,arguments)
// 	}
// })



module.exports=cache;