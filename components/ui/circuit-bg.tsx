'use client'

import { useEffect, useRef } from 'react'

const VERTEX_SHADER = `#version 300 es
precision highp float;
in vec2 position;
void main() {
  gl_Position = vec4(position, 0.0, 1.0);
}
`

const FRAGMENT_SHADER = `#version 300 es
precision highp float;
out vec4 fragColor;
uniform vec2 uResolution;
uniform float uTime;

float hash(vec2 p) {
  return fract(sin(dot(p, vec2(127.1, 311.7))) * 43758.5453);
}

float noise(vec2 p) {
  vec2 i = floor(p);
  vec2 f = fract(p);
  f = f * f * (3.0 - 2.0 * f);
  float a = hash(i);
  float b = hash(i + vec2(1.0, 0.0));
  float c = hash(i + vec2(0.0, 1.0));
  float d = hash(i + vec2(1.0, 1.0));
  return mix(mix(a, b, f.x), mix(c, d, f.x), f.y);
}

float gridLine(float coord, float width) {
  float d = abs(fract(coord) - 0.5) * 2.0;
  return smoothstep(width, 0.0, d);
}

float asciiPattern(vec2 uv) {
  vec2 cell = floor(uv);
  float h = hash(cell);
  vec2 f = fract(uv);
  float pattern = 0.0;
  if (h > 0.85) {
    pattern = step(0.4, f.x) * step(f.x, 0.6) + step(0.4, f.y) * step(f.y, 0.6);
  } else if (h > 0.7) {
    pattern = step(0.3, f.x) * step(f.x, 0.7) * step(0.3, f.y) * step(f.y, 0.7);
  } else if (h > 0.55) {
    float d = length(f - 0.5);
    pattern = smoothstep(0.25, 0.2, d);
  }
  return pattern * 0.40;
}

void main() {
  vec2 uv = gl_FragCoord.xy / uResolution;
  float aspect = uResolution.x / uResolution.y;

  vec3 c0 = vec3(0.18, 0.02, 0.02);
  vec3 c1 = vec3(0.28, 0.04, 0.04);
  vec3 c2 = vec3(0.12, 0.01, 0.01);
  vec3 c3 = vec3(0.35, 0.06, 0.06);

  float n1 = noise(uv * 3.0 + uTime * 0.05);
  float n2 = noise(uv * 5.0 - uTime * 0.03);
  vec3 bg = mix(mix(c0, c1, n1), mix(c2, c3, n2), uv.y);

  vec3 baseDeep = vec3(0.04, 0.01, 0.01);
  vec3 baseTint = vec3(0.08, 0.02, 0.02);
  bg = mix(baseDeep, bg, 0.7 + 0.3 * noise(uv * 2.0 + uTime * 0.02));
  bg = mix(bg, baseTint, 0.15);

  float scale = 18.0;
  vec2 gridUV = uv * scale * vec2(aspect, 1.0);

  vec3 lineThin = vec3(0.85, 0.15, 0.15);
  vec3 lineMajor = vec3(1.0, 1.0, 1.0);

  float thinX = gridLine(gridUV.x, 0.96);
  float thinY = gridLine(gridUV.y, 0.96);
  float thinGrid = max(thinX, thinY) * 0.08;

  float majorX = gridLine(gridUV.x / 4.0, 0.985);
  float majorY = gridLine(gridUV.y / 4.0, 0.985);
  float majorGrid = max(majorX, majorY) * 0.15;

  bg = mix(bg, lineThin, thinGrid);
  bg = mix(bg, lineMajor, majorGrid * 0.3);

  vec3 asciiColor = vec3(1.0, 0.12, 0.12);
  float ascii = asciiPattern(gridUV);
  float asciiPulse = 0.5 + 0.5 * sin(uTime * 0.5 + hash(floor(gridUV)) * 6.28);
  bg = mix(bg, asciiColor, ascii * asciiPulse * 0.3);

  float nodeHash = hash(floor(gridUV / 4.0));
  if (nodeHash > 0.7) {
    vec2 nodeCenter = (floor(gridUV / 4.0) + 0.5) * 4.0;
    float d = length(gridUV - nodeCenter);
    float glow = exp(-d * d * 0.8) * 0.4;
    float pulse = 0.6 + 0.4 * sin(uTime * 1.5 + nodeHash * 10.0);
    bg += vec3(0.9, 0.1, 0.1) * glow * pulse;
  }

  float vignette = 1.0 - 0.4 * length((uv - 0.5) * 1.4);
  bg *= vignette;

  fragColor = vec4(bg, 1.0);
}
`

export function CircuitBg() {
  const canvasRef = useRef<HTMLCanvasElement>(null)

  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas) return

    const gl = canvas.getContext('webgl2', { antialias: false, alpha: false })
    if (!gl) return

    const vertShader = gl.createShader(gl.VERTEX_SHADER)!
    gl.shaderSource(vertShader, VERTEX_SHADER)
    gl.compileShader(vertShader)

    const fragShader = gl.createShader(gl.FRAGMENT_SHADER)!
    gl.shaderSource(fragShader, FRAGMENT_SHADER)
    gl.compileShader(fragShader)

    const program = gl.createProgram()!
    gl.attachShader(program, vertShader)
    gl.attachShader(program, fragShader)
    gl.linkProgram(program)
    gl.useProgram(program)

    const vertices = new Float32Array([-1, -1, 1, -1, -1, 1, 1, 1])
    const buffer = gl.createBuffer()
    gl.bindBuffer(gl.ARRAY_BUFFER, buffer)
    gl.bufferData(gl.ARRAY_BUFFER, vertices, gl.STATIC_DRAW)

    const posLoc = gl.getAttribLocation(program, 'position')
    gl.enableVertexAttribArray(posLoc)
    gl.vertexAttribPointer(posLoc, 2, gl.FLOAT, false, 0, 0)

    const resLoc = gl.getUniformLocation(program, 'uResolution')
    const timeLoc = gl.getUniformLocation(program, 'uTime')

    let animationId: number
    const startTime = performance.now()

    const resize = () => {
      const dpr = Math.min(window.devicePixelRatio, 1.5)
      canvas.width = canvas.clientWidth * dpr
      canvas.height = canvas.clientHeight * dpr
      gl.viewport(0, 0, canvas.width, canvas.height)
    }

    const render = () => {
      resize()
      const elapsed = (performance.now() - startTime) / 1000
      gl.uniform2f(resLoc, canvas.width, canvas.height)
      gl.uniform1f(timeLoc, elapsed)
      gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4)
      animationId = requestAnimationFrame(render)
    }

    render()

    return () => {
      cancelAnimationFrame(animationId)
      gl.deleteProgram(program)
      gl.deleteShader(vertShader)
      gl.deleteShader(fragShader)
      gl.deleteBuffer(buffer)
    }
  }, [])

  return (
    <canvas
      ref={canvasRef}
      className="absolute inset-0 w-full h-full"
      style={{ opacity: 0.85 }}
    />
  )
}
