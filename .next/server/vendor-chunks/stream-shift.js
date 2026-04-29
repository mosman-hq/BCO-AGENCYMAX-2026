/*
 * ATTENTION: An "eval-source-map" devtool has been used.
 * This devtool is neither made for production nor for readable output files.
 * It uses "eval()" calls to create a separate source file with attached SourceMaps in the browser devtools.
 * If you are trying to read the output file, select a different devtool (https://webpack.js.org/configuration/devtool/)
 * or disable the default devtool with "devtool: false".
 * If you are looking for production-ready output files, see mode: "production" (https://webpack.js.org/configuration/mode/).
 */
exports.id = "vendor-chunks/stream-shift";
exports.ids = ["vendor-chunks/stream-shift"];
exports.modules = {

/***/ "(rsc)/./node_modules/stream-shift/index.js":
/*!********************************************!*\
  !*** ./node_modules/stream-shift/index.js ***!
  \********************************************/
/***/ ((module) => {

eval("module.exports = shift\n\nfunction shift (stream) {\n  var rs = stream._readableState\n  if (!rs) return null\n  return (rs.objectMode || typeof stream._duplexState === 'number') ? stream.read() : stream.read(getStateLength(rs))\n}\n\nfunction getStateLength (state) {\n  if (state.buffer.length) {\n    var idx = state.bufferIndex || 0\n    // Since node 6.3.0 state.buffer is a BufferList not an array\n    if (state.buffer.head) {\n      return state.buffer.head.data.length\n    } else if (state.buffer.length - idx > 0 && state.buffer[idx]) {\n      return state.buffer[idx].length\n    }\n  }\n\n  return state.length\n}\n//# sourceURL=[module]\n//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiKHJzYykvLi9ub2RlX21vZHVsZXMvc3RyZWFtLXNoaWZ0L2luZGV4LmpzIiwibWFwcGluZ3MiOiJBQUFBOztBQUVBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7O0FBRUE7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0EsTUFBTTtBQUNOO0FBQ0E7QUFDQTs7QUFFQTtBQUNBIiwic291cmNlcyI6WyJ3ZWJwYWNrOi8vYWdlbmN5LXpvbWJpZS8uL25vZGVfbW9kdWxlcy9zdHJlYW0tc2hpZnQvaW5kZXguanM/ZjExNSJdLCJzb3VyY2VzQ29udGVudCI6WyJtb2R1bGUuZXhwb3J0cyA9IHNoaWZ0XG5cbmZ1bmN0aW9uIHNoaWZ0IChzdHJlYW0pIHtcbiAgdmFyIHJzID0gc3RyZWFtLl9yZWFkYWJsZVN0YXRlXG4gIGlmICghcnMpIHJldHVybiBudWxsXG4gIHJldHVybiAocnMub2JqZWN0TW9kZSB8fCB0eXBlb2Ygc3RyZWFtLl9kdXBsZXhTdGF0ZSA9PT0gJ251bWJlcicpID8gc3RyZWFtLnJlYWQoKSA6IHN0cmVhbS5yZWFkKGdldFN0YXRlTGVuZ3RoKHJzKSlcbn1cblxuZnVuY3Rpb24gZ2V0U3RhdGVMZW5ndGggKHN0YXRlKSB7XG4gIGlmIChzdGF0ZS5idWZmZXIubGVuZ3RoKSB7XG4gICAgdmFyIGlkeCA9IHN0YXRlLmJ1ZmZlckluZGV4IHx8IDBcbiAgICAvLyBTaW5jZSBub2RlIDYuMy4wIHN0YXRlLmJ1ZmZlciBpcyBhIEJ1ZmZlckxpc3Qgbm90IGFuIGFycmF5XG4gICAgaWYgKHN0YXRlLmJ1ZmZlci5oZWFkKSB7XG4gICAgICByZXR1cm4gc3RhdGUuYnVmZmVyLmhlYWQuZGF0YS5sZW5ndGhcbiAgICB9IGVsc2UgaWYgKHN0YXRlLmJ1ZmZlci5sZW5ndGggLSBpZHggPiAwICYmIHN0YXRlLmJ1ZmZlcltpZHhdKSB7XG4gICAgICByZXR1cm4gc3RhdGUuYnVmZmVyW2lkeF0ubGVuZ3RoXG4gICAgfVxuICB9XG5cbiAgcmV0dXJuIHN0YXRlLmxlbmd0aFxufVxuIl0sIm5hbWVzIjpbXSwic291cmNlUm9vdCI6IiJ9\n//# sourceURL=webpack-internal:///(rsc)/./node_modules/stream-shift/index.js\n");

/***/ })

};
;