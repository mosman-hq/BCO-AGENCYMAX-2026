"use strict";
/*
 * ATTENTION: An "eval-source-map" devtool has been used.
 * This devtool is neither made for production nor for readable output files.
 * It uses "eval()" calls to create a separate source file with attached SourceMaps in the browser devtools.
 * If you are trying to read the output file, select a different devtool (https://webpack.js.org/configuration/devtool/)
 * or disable the default devtool with "devtool: false".
 * If you are looking for production-ready output files, see mode: "production" (https://webpack.js.org/configuration/mode/).
 */
exports.id = "vendor-chunks/stubs";
exports.ids = ["vendor-chunks/stubs"];
exports.modules = {

/***/ "(rsc)/./node_modules/stubs/index.js":
/*!*************************************!*\
  !*** ./node_modules/stubs/index.js ***!
  \*************************************/
/***/ ((module) => {

eval("\n\nmodule.exports = function stubs(obj, method, cfg, stub) {\n  if (!obj || !method || !obj[method])\n    throw new Error('You must provide an object and a key for an existing method')\n\n  if (!stub) {\n    stub = cfg\n    cfg = {}\n  }\n\n  stub = stub || function() {}\n\n  cfg.callthrough = cfg.callthrough || false\n  cfg.calls = cfg.calls || 0\n\n  var norevert = cfg.calls === 0\n\n  var cached = obj[method].bind(obj)\n\n  obj[method] = function() {\n    var args = [].slice.call(arguments)\n    var returnVal\n\n    if (cfg.callthrough)\n      returnVal = cached.apply(obj, args)\n\n    returnVal = stub.apply(obj, args) || returnVal\n\n    if (!norevert && --cfg.calls === 0)\n      obj[method] = cached\n\n    return returnVal\n  }\n}\n//# sourceURL=[module]\n//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiKHJzYykvLi9ub2RlX21vZHVsZXMvc3R1YnMvaW5kZXguanMiLCJtYXBwaW5ncyI6IkFBQVk7O0FBRVo7QUFDQTtBQUNBOztBQUVBO0FBQ0E7QUFDQTtBQUNBOztBQUVBOztBQUVBO0FBQ0E7O0FBRUE7O0FBRUE7O0FBRUE7QUFDQTtBQUNBOztBQUVBO0FBQ0E7O0FBRUE7O0FBRUE7QUFDQTs7QUFFQTtBQUNBO0FBQ0EiLCJzb3VyY2VzIjpbIndlYnBhY2s6Ly9hZ2VuY3ktem9tYmllLy4vbm9kZV9tb2R1bGVzL3N0dWJzL2luZGV4LmpzP2Q3ZjYiXSwic291cmNlc0NvbnRlbnQiOlsiJ3VzZSBzdHJpY3QnXG5cbm1vZHVsZS5leHBvcnRzID0gZnVuY3Rpb24gc3R1YnMob2JqLCBtZXRob2QsIGNmZywgc3R1Yikge1xuICBpZiAoIW9iaiB8fCAhbWV0aG9kIHx8ICFvYmpbbWV0aG9kXSlcbiAgICB0aHJvdyBuZXcgRXJyb3IoJ1lvdSBtdXN0IHByb3ZpZGUgYW4gb2JqZWN0IGFuZCBhIGtleSBmb3IgYW4gZXhpc3RpbmcgbWV0aG9kJylcblxuICBpZiAoIXN0dWIpIHtcbiAgICBzdHViID0gY2ZnXG4gICAgY2ZnID0ge31cbiAgfVxuXG4gIHN0dWIgPSBzdHViIHx8IGZ1bmN0aW9uKCkge31cblxuICBjZmcuY2FsbHRocm91Z2ggPSBjZmcuY2FsbHRocm91Z2ggfHwgZmFsc2VcbiAgY2ZnLmNhbGxzID0gY2ZnLmNhbGxzIHx8IDBcblxuICB2YXIgbm9yZXZlcnQgPSBjZmcuY2FsbHMgPT09IDBcblxuICB2YXIgY2FjaGVkID0gb2JqW21ldGhvZF0uYmluZChvYmopXG5cbiAgb2JqW21ldGhvZF0gPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgYXJncyA9IFtdLnNsaWNlLmNhbGwoYXJndW1lbnRzKVxuICAgIHZhciByZXR1cm5WYWxcblxuICAgIGlmIChjZmcuY2FsbHRocm91Z2gpXG4gICAgICByZXR1cm5WYWwgPSBjYWNoZWQuYXBwbHkob2JqLCBhcmdzKVxuXG4gICAgcmV0dXJuVmFsID0gc3R1Yi5hcHBseShvYmosIGFyZ3MpIHx8IHJldHVyblZhbFxuXG4gICAgaWYgKCFub3JldmVydCAmJiAtLWNmZy5jYWxscyA9PT0gMClcbiAgICAgIG9ialttZXRob2RdID0gY2FjaGVkXG5cbiAgICByZXR1cm4gcmV0dXJuVmFsXG4gIH1cbn1cbiJdLCJuYW1lcyI6W10sInNvdXJjZVJvb3QiOiIifQ==\n//# sourceURL=webpack-internal:///(rsc)/./node_modules/stubs/index.js\n");

/***/ })

};
;