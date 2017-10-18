var crc32 = require('fast-crc32c');
var _ = require('lodash');
function hash(){
	if (arguments.length==1 && (typeof arguments[0] =="string"))
		return arguments[0];
	
	return _.map(arguments,function(v){
		return _.isString(v) ? v:crc32.calculate(_.isFunction(v) ? v.toString():JSON.stringify(v))
	}).join(':::');
}
module.exports= hash;

