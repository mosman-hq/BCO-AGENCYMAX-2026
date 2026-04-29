"use strict";
/*
 * ATTENTION: An "eval-source-map" devtool has been used.
 * This devtool is neither made for production nor for readable output files.
 * It uses "eval()" calls to create a separate source file with attached SourceMaps in the browser devtools.
 * If you are trying to read the output file, select a different devtool (https://webpack.js.org/configuration/devtool/)
 * or disable the default devtool with "devtool: false".
 * If you are looking for production-ready output files, see mode: "production" (https://webpack.js.org/configuration/mode/).
 */
exports.id = "vendor-chunks/stream-events";
exports.ids = ["vendor-chunks/stream-events"];
exports.modules = {

/***/ "(rsc)/./node_modules/stream-events/index.js":
/*!*********************************************!*\
  !*** ./node_modules/stream-events/index.js ***!
  \*********************************************/
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

eval("\n\nvar stubs = __webpack_require__(/*! stubs */ \"(rsc)/./node_modules/stubs/index.js\")\n\n/*\n * StreamEvents can be used 2 ways:\n *\n * 1:\n * function MyStream() {\n *   require('stream-events').call(this)\n * }\n *\n * 2:\n * require('stream-events')(myStream)\n */\nfunction StreamEvents(stream) {\n  stream = stream || this\n\n  var cfg = {\n    callthrough: true,\n    calls: 1\n  }\n\n  stubs(stream, '_read', cfg, stream.emit.bind(stream, 'reading'))\n  stubs(stream, '_write', cfg, stream.emit.bind(stream, 'writing'))\n\n  return stream\n}\n\nmodule.exports = StreamEvents\n//# sourceURL=[module]\n//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiKHJzYykvLi9ub2RlX21vZHVsZXMvc3RyZWFtLWV2ZW50cy9pbmRleC5qcyIsIm1hcHBpbmdzIjoiQUFBYTs7QUFFYixZQUFZLG1CQUFPLENBQUMsa0RBQU87O0FBRTNCO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBOztBQUVBO0FBQ0E7QUFDQTtBQUNBOztBQUVBO0FBQ0E7O0FBRUE7QUFDQTs7QUFFQSIsInNvdXJjZXMiOlsid2VicGFjazovL2FnZW5jeS16b21iaWUvLi9ub2RlX21vZHVsZXMvc3RyZWFtLWV2ZW50cy9pbmRleC5qcz9iMWY4Il0sInNvdXJjZXNDb250ZW50IjpbIid1c2Ugc3RyaWN0JztcblxudmFyIHN0dWJzID0gcmVxdWlyZSgnc3R1YnMnKVxuXG4vKlxuICogU3RyZWFtRXZlbnRzIGNhbiBiZSB1c2VkIDIgd2F5czpcbiAqXG4gKiAxOlxuICogZnVuY3Rpb24gTXlTdHJlYW0oKSB7XG4gKiAgIHJlcXVpcmUoJ3N0cmVhbS1ldmVudHMnKS5jYWxsKHRoaXMpXG4gKiB9XG4gKlxuICogMjpcbiAqIHJlcXVpcmUoJ3N0cmVhbS1ldmVudHMnKShteVN0cmVhbSlcbiAqL1xuZnVuY3Rpb24gU3RyZWFtRXZlbnRzKHN0cmVhbSkge1xuICBzdHJlYW0gPSBzdHJlYW0gfHwgdGhpc1xuXG4gIHZhciBjZmcgPSB7XG4gICAgY2FsbHRocm91Z2g6IHRydWUsXG4gICAgY2FsbHM6IDFcbiAgfVxuXG4gIHN0dWJzKHN0cmVhbSwgJ19yZWFkJywgY2ZnLCBzdHJlYW0uZW1pdC5iaW5kKHN0cmVhbSwgJ3JlYWRpbmcnKSlcbiAgc3R1YnMoc3RyZWFtLCAnX3dyaXRlJywgY2ZnLCBzdHJlYW0uZW1pdC5iaW5kKHN0cmVhbSwgJ3dyaXRpbmcnKSlcblxuICByZXR1cm4gc3RyZWFtXG59XG5cbm1vZHVsZS5leHBvcnRzID0gU3RyZWFtRXZlbnRzXG4iXSwibmFtZXMiOltdLCJzb3VyY2VSb290IjoiIn0=\n//# sourceURL=webpack-internal:///(rsc)/./node_modules/stream-events/index.js\n");

/***/ })

};
;