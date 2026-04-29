"use strict";
/*
 * ATTENTION: An "eval-source-map" devtool has been used.
 * This devtool is neither made for production nor for readable output files.
 * It uses "eval()" calls to create a separate source file with attached SourceMaps in the browser devtools.
 * If you are trying to read the output file, select a different devtool (https://webpack.js.org/configuration/devtool/)
 * or disable the default devtool with "devtool: false".
 * If you are looking for production-ready output files, see mode: "production" (https://webpack.js.org/configuration/mode/).
 */
exports.id = "vendor-chunks/@tootallnate";
exports.ids = ["vendor-chunks/@tootallnate"];
exports.modules = {

/***/ "(rsc)/./node_modules/@tootallnate/once/dist/index.js":
/*!******************************************************!*\
  !*** ./node_modules/@tootallnate/once/dist/index.js ***!
  \******************************************************/
/***/ ((__unused_webpack_module, exports) => {

eval("\nObject.defineProperty(exports, \"__esModule\", ({ value: true }));\nfunction once(emitter, name, { signal } = {}) {\n    return new Promise((resolve, reject) => {\n        function cleanup() {\n            signal === null || signal === void 0 ? void 0 : signal.removeEventListener('abort', cleanup);\n            emitter.removeListener(name, onEvent);\n            emitter.removeListener('error', onError);\n        }\n        function onEvent(...args) {\n            cleanup();\n            resolve(args);\n        }\n        function onError(err) {\n            cleanup();\n            reject(err);\n        }\n        signal === null || signal === void 0 ? void 0 : signal.addEventListener('abort', cleanup);\n        emitter.on(name, onEvent);\n        emitter.on('error', onError);\n    });\n}\nexports[\"default\"] = once;\n//# sourceMappingURL=index.js.map//# sourceURL=[module]\n//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiKHJzYykvLi9ub2RlX21vZHVsZXMvQHRvb3RhbGxuYXRlL29uY2UvZGlzdC9pbmRleC5qcyIsIm1hcHBpbmdzIjoiQUFBYTtBQUNiLDhDQUE2QyxFQUFFLGFBQWEsRUFBQztBQUM3RCwrQkFBK0IsU0FBUyxJQUFJO0FBQzVDO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSxLQUFLO0FBQ0w7QUFDQSxrQkFBZTtBQUNmIiwic291cmNlcyI6WyJ3ZWJwYWNrOi8vYWdlbmN5LXpvbWJpZS8uL25vZGVfbW9kdWxlcy9AdG9vdGFsbG5hdGUvb25jZS9kaXN0L2luZGV4LmpzPzQ4MTkiXSwic291cmNlc0NvbnRlbnQiOlsiXCJ1c2Ugc3RyaWN0XCI7XG5PYmplY3QuZGVmaW5lUHJvcGVydHkoZXhwb3J0cywgXCJfX2VzTW9kdWxlXCIsIHsgdmFsdWU6IHRydWUgfSk7XG5mdW5jdGlvbiBvbmNlKGVtaXR0ZXIsIG5hbWUsIHsgc2lnbmFsIH0gPSB7fSkge1xuICAgIHJldHVybiBuZXcgUHJvbWlzZSgocmVzb2x2ZSwgcmVqZWN0KSA9PiB7XG4gICAgICAgIGZ1bmN0aW9uIGNsZWFudXAoKSB7XG4gICAgICAgICAgICBzaWduYWwgPT09IG51bGwgfHwgc2lnbmFsID09PSB2b2lkIDAgPyB2b2lkIDAgOiBzaWduYWwucmVtb3ZlRXZlbnRMaXN0ZW5lcignYWJvcnQnLCBjbGVhbnVwKTtcbiAgICAgICAgICAgIGVtaXR0ZXIucmVtb3ZlTGlzdGVuZXIobmFtZSwgb25FdmVudCk7XG4gICAgICAgICAgICBlbWl0dGVyLnJlbW92ZUxpc3RlbmVyKCdlcnJvcicsIG9uRXJyb3IpO1xuICAgICAgICB9XG4gICAgICAgIGZ1bmN0aW9uIG9uRXZlbnQoLi4uYXJncykge1xuICAgICAgICAgICAgY2xlYW51cCgpO1xuICAgICAgICAgICAgcmVzb2x2ZShhcmdzKTtcbiAgICAgICAgfVxuICAgICAgICBmdW5jdGlvbiBvbkVycm9yKGVycikge1xuICAgICAgICAgICAgY2xlYW51cCgpO1xuICAgICAgICAgICAgcmVqZWN0KGVycik7XG4gICAgICAgIH1cbiAgICAgICAgc2lnbmFsID09PSBudWxsIHx8IHNpZ25hbCA9PT0gdm9pZCAwID8gdm9pZCAwIDogc2lnbmFsLmFkZEV2ZW50TGlzdGVuZXIoJ2Fib3J0JywgY2xlYW51cCk7XG4gICAgICAgIGVtaXR0ZXIub24obmFtZSwgb25FdmVudCk7XG4gICAgICAgIGVtaXR0ZXIub24oJ2Vycm9yJywgb25FcnJvcik7XG4gICAgfSk7XG59XG5leHBvcnRzLmRlZmF1bHQgPSBvbmNlO1xuLy8jIHNvdXJjZU1hcHBpbmdVUkw9aW5kZXguanMubWFwIl0sIm5hbWVzIjpbXSwic291cmNlUm9vdCI6IiJ9\n//# sourceURL=webpack-internal:///(rsc)/./node_modules/@tootallnate/once/dist/index.js\n");

/***/ })

};
;