// components/RadarWebGL.jsx
import { useEffect, useRef, useCallback } from 'react'

const API = ''

// ── Paleta NWS dBZ ─────────────────────────────────────────────────────────
const NWS_STOPS = [
  [-10,   4, 233, 231],
  [  0,   1, 159, 244],
  [  5,   3,   0, 244],
  [ 10,   2, 253,   2],
  [ 15,   1, 197,   1],
  [ 20,   0, 142,   0],
  [ 25, 253, 248,   2],
  [ 30, 229, 188,   0],
  [ 35, 253, 149,   0],
  [ 40, 253,   0,   0],
  [ 45, 212,   0,   0],
  [ 50, 188,   0,   0],
  [ 55, 248,   0, 253],
  [ 60, 152,  84, 198],
  [ 70, 255, 255, 255],
]

function buildPalette() {
  const out = new Uint8Array(256 * 4)
  for (let i = 0; i < 256; i++) {
    const dbz = -10 + (i / 255) * 80
    let r = NWS_STOPS[0][1], g = NWS_STOPS[0][2], b = NWS_STOPS[0][3]
    for (let s = 0; s < NWS_STOPS.length - 1; s++) {
      const [v0,r0,g0,b0] = NWS_STOPS[s]
      const [v1,r1,g1,b1] = NWS_STOPS[s+1]
      if (dbz >= v0 && dbz <= v1) {
        const t = (dbz-v0)/(v1-v0)
        r = Math.round(r0+t*(r1-r0))
        g = Math.round(g0+t*(g1-g0))
        b = Math.round(b0+t*(b1-b0))
        break
      }
      if (dbz > v1) { r=r1; g=g1; b=b1 }
    }
    out[i*4]=r; out[i*4+1]=g; out[i*4+2]=b; out[i*4+3]=255
  }
  return out
}

// Paleta prędkości radialnej: niebieski (vmin) → biały (0) → czerwony (vmax)
function buildVelocityPalette(vmin, vmax) {
  const out = new Uint8Array(256 * 4)
  for (let i = 0; i < 256; i++) {
    const v = vmin + (i / 255) * (vmax - vmin)
    let r, g, b
    if (v < 0) {
      const s = v / vmin  // 1 przy vmin, 0 przy 0
      r = Math.round(240 * (1 - s))
      g = Math.round(240 * (1 - s))
      b = Math.round(240 + (178 - 240) * (1 - s))
    } else {
      const s = vmax > 0 ? v / vmax : 0  // 0 przy 0, 1 przy vmax
      r = Math.round(240 + (140 - 240) * s)
      g = Math.round(240 * (1 - s))
      b = Math.round(240 * (1 - s))
    }
    out[i*4]=r; out[i*4+1]=g; out[i*4+2]=b; out[i*4+3]=255
  }
  return out
}

// ── Shaders ────────────────────────────────────────────────────────────────
const VERT = `
precision mediump float;
attribute vec2 a_pos;
attribute vec2 a_uv;
varying   vec2 v_uv;
void main() { v_uv=a_uv; gl_Position=vec4(a_pos,0.,1.); }
`

// Backend enkoduje:
//   val_raw = round((dbz - vmin)/(vmax-vmin) * 65534)   [0..65534]
//   R = val_raw >> 8    (high byte 0..255, uploadowany do WebGL)
//   G = val_raw & 0xFF  (low byte  0..255, uploadowany do WebGL)
//   A = 255 jeśli dane, 0 jeśli NaN
//
// WebGL UNSIGNED_BYTE texture → sampler zwraca float [0..1]:
//   d.r = R / 255.0,  d.g = G / 255.0
//
// Odtworzenie:
//   high_byte = d.r * 255.0    → float odpowiadający R_byte
//   low_byte  = d.g * 255.0    → float odpowiadający G_byte
//   val_raw   = high * 256 + low   → [0..65534]
//   dbz       = val_raw / 65534 * (vmax-vmin) + vmin
//
// UWAGA: floor(x + 0.5) zamiast round() bo GLSL nie ma round() w WebGL 1.0
const FRAG = `
precision mediump float;
uniform sampler2D u_data;
uniform sampler2D u_pal;
uniform float u_vmin;
uniform float u_vmax;
uniform float u_opacity;
varying vec2 v_uv;
void main() {
  vec4 d = texture2D(u_data, v_uv);
  if (d.a < 0.5) { gl_FragColor = vec4(0.0); return; }
  float high = floor(d.r * 255.0 + 0.5);
  float low  = floor(d.g * 255.0 + 0.5);
  float raw  = high * 256.0 + low;
  float dbz  = raw / 65534.0 * (u_vmax - u_vmin) + u_vmin;
  float t    = clamp((dbz - u_vmin) / (u_vmax - u_vmin), 0.0, 1.0);
  vec4  c    = texture2D(u_pal, vec2(t, 0.5));
  gl_FragColor = vec4(c.rgb, u_opacity);
}
`

// ── Renderer ───────────────────────────────────────────────────────────────
function mkShader(gl, type, src) {
  const s = gl.createShader(type)
  gl.shaderSource(s, src); gl.compileShader(s)
  if (!gl.getShaderParameter(s, gl.COMPILE_STATUS))
    console.error('shader:', gl.getShaderInfoLog(s))
  return s
}

function mkProgram(gl, vs, fs) {
  const p = gl.createProgram()
  gl.attachShader(p, mkShader(gl, gl.VERTEX_SHADER, vs))
  gl.attachShader(p, mkShader(gl, gl.FRAGMENT_SHADER, fs))
  gl.linkProgram(p)
  if (!gl.getProgramParameter(p, gl.LINK_STATUS))
    console.error('program:', gl.getProgramInfoLog(p))
  return p
}

function b64toF32(b64) {
  if (!b64) return null
  const bin = atob(b64), buf = new ArrayBuffer(bin.length)
  const u8 = new Uint8Array(buf)
  for (let i = 0; i < bin.length; i++) u8[i] = bin.charCodeAt(i)
  return new Float32Array(buf)
}

function b64toU8(b64) {
  if (!b64) return null
  const bin = atob(b64), buf = new ArrayBuffer(bin.length)
  const u8 = new Uint8Array(buf)
  for (let i = 0; i < bin.length; i++) u8[i] = bin.charCodeAt(i)
  return u8
}

class Renderer {
  constructor(container) {
    const cv = document.createElement('canvas')
    cv.style.cssText = 'position:absolute;top:0;left:0;pointer-events:none;z-index:300'
    container.style.position = container.style.position || 'relative'
    container.appendChild(cv)
    this.cv = cv

    const gl = cv.getContext('webgl', {
      antialias: false, premultipliedAlpha: false, preserveDrawingBuffer: false,
    })
    if (!gl) throw new Error('WebGL brak')
    this.gl = gl
    this.ext = gl.getExtension('OES_element_index_uint')

    this.prog = mkProgram(gl, VERT, FRAG)
    this.aPos = gl.getAttribLocation(this.prog, 'a_pos')
    this.aUV  = gl.getAttribLocation(this.prog, 'a_uv')
    this.uData    = gl.getUniformLocation(this.prog, 'u_data')
    this.uPal     = gl.getUniformLocation(this.prog, 'u_pal')
    this.uVmin    = gl.getUniformLocation(this.prog, 'u_vmin')
    this.uVmax    = gl.getUniformLocation(this.prog, 'u_vmax')
    this.uOpacity = gl.getUniformLocation(this.prog, 'u_opacity')

    this.bPos = gl.createBuffer()
    this.bUV  = gl.createBuffer()
    this.bIdx = gl.createBuffer()

    // Paleta 256×1
    const pt = gl.createTexture()
    gl.bindTexture(gl.TEXTURE_2D, pt)
    gl.texImage2D(gl.TEXTURE_2D,0,gl.RGBA,256,1,0,gl.RGBA,gl.UNSIGNED_BYTE,buildPalette())
    gl.texParameteri(gl.TEXTURE_2D,gl.TEXTURE_MIN_FILTER,gl.LINEAR)
    gl.texParameteri(gl.TEXTURE_2D,gl.TEXTURE_MAG_FILTER,gl.LINEAR)
    gl.texParameteri(gl.TEXTURE_2D,gl.TEXTURE_WRAP_S,gl.CLAMP_TO_EDGE)
    gl.texParameteri(gl.TEXTURE_2D,gl.TEXTURE_WRAP_T,gl.CLAMP_TO_EDGE)
    this.tPal = pt

    gl.enable(gl.BLEND)
    gl.blendFunc(gl.SRC_ALPHA, gl.ONE_MINUS_SRC_ALPHA)

    this.tData    = null
    this.proj     = null
    this.lats     = null
    this.lons     = null
    this.opacity  = 0.85
    this.idxReady = false
    this.triCount = 0
    this.map      = null
  }

  updatePalette(pixels) {
    const gl = this.gl
    gl.bindTexture(gl.TEXTURE_2D, this.tPal)
    gl.texImage2D(gl.TEXTURE_2D,0,gl.RGBA,256,1,0,gl.RGBA,gl.UNSIGNED_BYTE,pixels)
  }

  destroy() {
    this.cv.remove()
    const gl = this.gl
    gl.deleteProgram(this.prog)
    ;[this.bPos, this.bUV, this.bIdx].forEach(b => gl.deleteBuffer(b))
    if (this.tData) gl.deleteTexture(this.tData)
    gl.deleteTexture(this.tPal)
  }

  setOpacity(v) {
    this.opacity = v
    if (this.map) this.render(this.map)
  }

  // data_b64: raw RGBA bytes (nie PNG!) — flat Uint8Array, row-major
  loadData(data_b64, proj) {
    this.proj     = proj
    this.idxReady = false
    this.triCount = 0
    this.lats     = b64toF32(proj.mesh_lats)
    this.lons     = b64toF32(proj.mesh_lons)

    const q = (proj.quantity ?? '').toUpperCase()
    const isVelocity = q === 'VRAD' || q === 'V' || q === 'VRADDH' || q === 'VRADS'
                    || (proj.product ?? '').toUpperCase().endsWith('CAPPI_V')
    if (isVelocity) {
      this.updatePalette(buildVelocityPalette(proj.vmin, proj.vmax))
    } else {
      this.updatePalette(buildPalette())
    }

    const gl     = this.gl
    const pixels = b64toU8(data_b64)   // Uint8Array RGBA flat

    if (this.tData) gl.deleteTexture(this.tData)
    const tex = gl.createTexture()
    gl.bindTexture(gl.TEXTURE_2D, tex)
    // Uploaduj raw RGBA bytes bezpośrednio — zero konwersji
    gl.texImage2D(
      gl.TEXTURE_2D, 0, gl.RGBA,
      proj.xsize, proj.ysize, 0,
      gl.RGBA, gl.UNSIGNED_BYTE, pixels
    )
    gl.texParameteri(gl.TEXTURE_2D,gl.TEXTURE_MIN_FILTER,gl.NEAREST)
    gl.texParameteri(gl.TEXTURE_2D,gl.TEXTURE_MAG_FILTER,gl.NEAREST)
    gl.texParameteri(gl.TEXTURE_2D,gl.TEXTURE_WRAP_S,gl.CLAMP_TO_EDGE)
    gl.texParameteri(gl.TEXTURE_2D,gl.TEXTURE_WRAP_T,gl.CLAMP_TO_EDGE)
    this.tData = tex

    if (this.map) this.render(this.map)
  }

  render(map) {
    this.map = map
    if (!this.tData || !this.proj || !this.lats) return

    const gl   = this.gl
    const size = map.getSize()
    const W    = size.x, H = size.y

    if (this.cv.width !== W || this.cv.height !== H) {
      this.cv.width = W; this.cv.height = H
    }
    gl.viewport(0, 0, W, H)
    gl.clearColor(0,0,0,0)
    gl.clear(gl.COLOR_BUFFER_BIT)

    const { mesh_rows: rows, mesh_cols: cols, mesh_step: step,
            xsize, ysize, vmin, vmax } = this.proj

    const pos = new Float32Array(rows * cols * 2)
    const uv  = new Float32Array(rows * cols * 2)

    for (let r = 0; r < rows; r++) {
      for (let c = 0; c < cols; c++) {
        const i  = r*cols+c
        const pt = map.latLngToContainerPoint([this.lats[i], this.lons[i]])
        pos[i*2]   =  (pt.x/W)*2 - 1
        pos[i*2+1] = -(pt.y/H)*2 + 1
        uv[i*2]    = (c*step)/xsize
        uv[i*2+1]  = (r*step)/ysize
      }
    }

    if (!this.idxReady) {
      const tc  = (rows-1)*(cols-1)*2
      const idx = new Uint32Array(tc*3)
      let k = 0
      for (let r = 0; r < rows-1; r++) {
        for (let c = 0; c < cols-1; c++) {
          const tl = r*cols+c
          idx[k++]=tl;     idx[k++]=tl+cols;   idx[k++]=tl+1
          idx[k++]=tl+cols;idx[k++]=tl+cols+1; idx[k++]=tl+1
        }
      }
      gl.bindBuffer(gl.ELEMENT_ARRAY_BUFFER, this.bIdx)
      gl.bufferData(gl.ELEMENT_ARRAY_BUFFER, idx, gl.STATIC_DRAW)
      this.triCount = tc
      this.idxReady = true
    }

    const p = this.prog
    gl.useProgram(p)

    gl.bindBuffer(gl.ARRAY_BUFFER, this.bPos)
    gl.bufferData(gl.ARRAY_BUFFER, pos, gl.DYNAMIC_DRAW)
    gl.enableVertexAttribArray(this.aPos)
    gl.vertexAttribPointer(this.aPos, 2, gl.FLOAT, false, 0, 0)

    gl.bindBuffer(gl.ARRAY_BUFFER, this.bUV)
    gl.bufferData(gl.ARRAY_BUFFER, uv, gl.DYNAMIC_DRAW)
    gl.enableVertexAttribArray(this.aUV)
    gl.vertexAttribPointer(this.aUV, 2, gl.FLOAT, false, 0, 0)

    gl.bindBuffer(gl.ELEMENT_ARRAY_BUFFER, this.bIdx)

    gl.activeTexture(gl.TEXTURE0)
    gl.bindTexture(gl.TEXTURE_2D, this.tData)
    gl.uniform1i(this.uData, 0)

    gl.activeTexture(gl.TEXTURE1)
    gl.bindTexture(gl.TEXTURE_2D, this.tPal)
    gl.uniform1i(this.uPal, 1)

    gl.uniform1f(this.uVmin,    vmin)
    gl.uniform1f(this.uVmax,    vmax)
    gl.uniform1f(this.uOpacity, this.opacity)

    gl.drawElements(
      gl.TRIANGLES, this.triCount*3,
      this.ext ? gl.UNSIGNED_INT : gl.UNSIGNED_SHORT, 0
    )
  }
}

// ── React komponent ────────────────────────────────────────────────────────
export default function RadarWebGL({ product, selectedTs, map, opacity = 0.85, onProjLoad }) {
  const rendRef  = useRef(null)
  const abortRef = useRef(null)

  useEffect(() => {
    if (!map) return
    const r = new Renderer(map.getContainer())
    rendRef.current = r
    const upd = () => r.render(map)
    map.on('move zoom viewreset resize', upd)
    return () => {
      map.off('move zoom viewreset resize', upd)
      r.destroy(); rendRef.current = null
    }
  }, [map])

  useEffect(() => { rendRef.current?.setOpacity(opacity) }, [opacity])

  const fetch_ = useCallback(async () => {
    if (!product || !rendRef.current) return
    abortRef.current?.abort()
    abortRef.current = new AbortController()
    try {
      const qs  = selectedTs
        ? `?scan_time=${encodeURIComponent(selectedTs)}`
        : ''
      const res = await fetch(
        `${API}/api/radar/${product}/webgl${qs}`,
        { signal: abortRef.current.signal }
      )
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      const json = await res.json()
      // Backend zwraca data_b64 (raw RGBA) lub texture_b64 (PNG) — obsłuż oba
      rendRef.current?.loadData(json.data_b64 ?? json.texture_b64, { ...json.proj, product })
      onProjLoad?.(json.proj)
    } catch (e) {
      if (e.name !== 'AbortError') console.error('RadarWebGL:', e.message)
    }
  }, [product, selectedTs])

  useEffect(() => {
    fetch_()
    return () => abortRef.current?.abort()
  }, [fetch_])

  return null
}
