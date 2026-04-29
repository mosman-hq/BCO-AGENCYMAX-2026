"use strict";
/*
 * ATTENTION: An "eval-source-map" devtool has been used.
 * This devtool is neither made for production nor for readable output files.
 * It uses "eval()" calls to create a separate source file with attached SourceMaps in the browser devtools.
 * If you are trying to read the output file, select a different devtool (https://webpack.js.org/configuration/devtool/)
 * or disable the default devtool with "devtool: false".
 * If you are looking for production-ready output files, see mode: "production" (https://webpack.js.org/configuration/mode/).
 */
exports.id = "vendor-chunks/arrify";
exports.ids = ["vendor-chunks/arrify"];
exports.modules = {

/***/ "(rsc)/./node_modules/arrify/index.js":
/*!**************************************!*\
  !*** ./node_modules/arrify/index.js ***!
  \**************************************/
/***/ ((module) => {

eval("\n\nconst arrify = value => {\n\tif (value === null || value === undefined) {\n\t\treturn [];\n\t}\n\n\tif (Array.isArray(value)) {\n\t\treturn value;\n\t}\n\n\tif (typeof value === 'string') {\n\t\treturn [value];\n\t}\n\n\tif (typeof value[Symbol.iterator] === 'function') {\n\t\treturn [...value];\n\t}\n\n\treturn [value];\n};\n\nmodule.exports = arrify;\n//# sourceURL=[module]\n//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiKHJzYykvLi9ub2RlX21vZHVsZXMvYXJyaWZ5L2luZGV4LmpzIiwibWFwcGluZ3MiOiJBQUFhOztBQUViO0FBQ0E7QUFDQTtBQUNBOztBQUVBO0FBQ0E7QUFDQTs7QUFFQTtBQUNBO0FBQ0E7O0FBRUE7QUFDQTtBQUNBOztBQUVBO0FBQ0E7O0FBRUEiLCJzb3VyY2VzIjpbIndlYnBhY2s6Ly9hZ2VuY3ktem9tYmllLy4vbm9kZV9tb2R1bGVzL2FycmlmeS9pbmRleC5qcz9iMTc2Il0sInNvdXJjZXNDb250ZW50IjpbIid1c2Ugc3RyaWN0JztcblxuY29uc3QgYXJyaWZ5ID0gdmFsdWUgPT4ge1xuXHRpZiAodmFsdWUgPT09IG51bGwgfHwgdmFsdWUgPT09IHVuZGVmaW5lZCkge1xuXHRcdHJldHVybiBbXTtcblx0fVxuXG5cdGlmIChBcnJheS5pc0FycmF5KHZhbHVlKSkge1xuXHRcdHJldHVybiB2YWx1ZTtcblx0fVxuXG5cdGlmICh0eXBlb2YgdmFsdWUgPT09ICdzdHJpbmcnKSB7XG5cdFx0cmV0dXJuIFt2YWx1ZV07XG5cdH1cblxuXHRpZiAodHlwZW9mIHZhbHVlW1N5bWJvbC5pdGVyYXRvcl0gPT09ICdmdW5jdGlvbicpIHtcblx0XHRyZXR1cm4gWy4uLnZhbHVlXTtcblx0fVxuXG5cdHJldHVybiBbdmFsdWVdO1xufTtcblxubW9kdWxlLmV4cG9ydHMgPSBhcnJpZnk7XG4iXSwibmFtZXMiOltdLCJzb3VyY2VSb290IjoiIn0=\n//# sourceURL=webpack-internal:///(rsc)/./node_modules/arrify/index.js\n");

/***/ })

};
;