export const fireEngineBrandingScript = `// Run inside the page context: await page.evaluate(() => { ... });
(async function __extractBrandDesign() {
  const clamp = (n, min, max) => Math.max(min, Math.min(max, n));
  const toPx = v => {
    if (!v || v === 'auto') return null;
    if (v.endsWith('px')) return parseFloat(v);
    if (v.endsWith('rem')) return parseFloat(v) * parseFloat(getComputedStyle(document.documentElement).fontSize || 16);
    if (v.endsWith('em')) return parseFloat(v) * parseFloat(getComputedStyle(document.body).fontSize || 16);
    if (v.endsWith('%')) return null;
    const num = parseFloat(v);
    return Number.isFinite(num) ? num : null;
  };

  const dedupe = arr => Array.from(new Set(arr.filter(Boolean)));
  const hexify = (rgba) => {
    if (!rgba) return null;
    if (/^#([0-9a-f]{3,8})$/i.test(rgba)) {
      if (rgba.length === 4) {
        return '#' + [...rgba.slice(1)].map(ch => ch + ch).join('').toUpperCase();
      }
      if (rgba.length === 7) return rgba.toUpperCase();
      if (rgba.length === 9) return rgba.slice(0,7).toUpperCase();
      return rgba.toUpperCase();
    }
    const ctx = document.createElement('canvas').getContext('2d');
    if (!ctx) return null;
    ctx.fillStyle = rgba;
    const val = ctx.fillStyle;
    const m = val.match(/^rgba?\((\d+),\s*(\d+),\s*(\d+)/i);
    if (!m) return null;
    const [r,g,b] = m.slice(1,4).map(n => clamp(parseInt(n,10),0,255));
    return '#' + [r,g,b].map(x => x.toString(16).padStart(2,'0')).join('').toUpperCase();
  };

  const contrastYIQ = (hex) => {
    if (!hex) return 0;
    const h = hex.replace('#','');
    if (h.length < 6) return 0;
    const r = parseInt(h.slice(0,2),16), g = parseInt(h.slice(2,4),16), b = parseInt(h.slice(4,6),16);
    return (r*299 + g*587 + b*114)/1000;
  };

  const collectSameOriginRules = () => {
    const data = {
      cssVars: {},
      colorsInCSS: [],
      fontsInCSS: [],
      fontFaces: [],
      hoverRules: [],
      transitions: [],
      gridMaxWidths: [],
      radii: [],
      spacings: [],
      iconHints: [],
      buttonRules: [],
      inputRules: [],
      cardRules: [],
      codeFonts: []
    };
    const pushColor = c => { const h = hexify(c); if (h) data.colorsInCSS.push(h); };
    const pushFont = f => {
      if (!f) return;
      const fam = String(f).split(',')[0]?.replace(/["']/g,'').trim();
      if (fam) data.fontsInCSS.push(fam);
    };

    for (const sheet of Array.from(document.styleSheets)) {
      let rules;
      try {
        rules = sheet.cssRules;
      } catch (e) {
        continue;
      }
      if (!rules) continue;

      for (const rule of Array.from(rules)) {
        try {
          if (rule.type === CSSRule.STYLE_RULE) {
            const s = rule.style;
            for (const name of Array.from(s)) {
              if (name.startsWith('--')) {
                data.cssVars[name] = s.getPropertyValue(name).trim();
                pushColor(data.cssVars[name]);
              }
            }
            ['color','background','background-color','border-color','border-top-color','border-right-color','border-bottom-color','border-left-color','outline-color','fill','stroke']
              .forEach(prop => pushColor(s.getPropertyValue(prop)));
            ['font','font-family'].forEach(prop => {
              const val = s.getPropertyValue(prop);
              if (val) {
                const fam = val.split(',')[0]?.replace(/["']/g,'').trim();
                if (fam) data.fontsInCSS.push(fam);
                if (/monospace|code|fira|ibm plex mono|jetbrains|menlo|consolas/i.test(val)) data.codeFonts.push(fam);
              }
            });
            ['border-radius','border-top-left-radius','border-top-right-radius','border-bottom-left-radius','border-bottom-right-radius'].forEach(p => {
              const v = toPx(s.getPropertyValue(p)); if (v) data.radii.push(v);
            });
            ['margin','margin-top','margin-right','margin-bottom','margin-left','padding','padding-top','padding-right','padding-bottom','padding-left','gap','grid-gap','row-gap','column-gap'].forEach(p => {
              const v = toPx(s.getPropertyValue(p)); if (v) data.spacings.push(v);
            });
            const td = s.getPropertyValue('transition-duration');
            const te = s.getPropertyValue('transition-timing-function');
            if (td) data.transitions.push({duration: td.trim(), easing: te?.trim() || ''});
            if (/(container|wrapper)/i.test(rule.selectorText || '') || /max-width/i.test(s.cssText)) {
              const mw = s.getPropertyValue('max-width'); if (mw) data.gridMaxWidths.push(mw.trim());
            }
            if ((rule.selectorText || '').includes(':hover')) data.hoverRules.push(rule.selectorText);
            const sel = (rule.selectorText || '');
            if (/(^|[\s.:#])btn([-\w]|$)|button/i.test(sel)) data.buttonRules.push(sel);
            if (/(^|[\s.:#])(input|textfield|form-control|text-field|select|textarea)/i.test(sel)) data.inputRules.push(sel);
            if (/(^|[\s.:#])(card|panel|tile|box|paper|shadow)/i.test(sel)) data.cardRules.push(sel);
          } else if (rule.type === CSSRule.FONT_FACE_RULE) {
            const s = rule.style;
            const fam = s.getPropertyValue('font-family')?.replace(/["']/g,'').trim();
            const src = s.getPropertyValue('src') || '';
            data.fontFaces.push({family: fam || '', src});
            if (fam) data.fontsInCSS.push(fam);
          }
        } catch {}
      }
    }
    const links = Array.from(document.querySelectorAll('link[rel="stylesheet"],link[as="style"],link[href]'));
    const scripts = Array.from(document.querySelectorAll('script[src]'));
    const hrefs = links.map(l=>l.href).concat(scripts.map(s=>s.src)).join(' ');
    if (/fontawesome|kit\.fontawesome/i.test(hrefs)) data.iconHints.push('fontawesome');
    if (/material(\.io|icons|symbol)/i.test(hrefs)) data.iconHints.push('material-icons');
    if (/heroicons|lucide|feather-icons|remixicon|ionicons/i.test(hrefs)) data.iconHints.push('icon-library');

    return data;
  };

  const sampleNodes = () => {
    const picks = [];
    const pushQ = (q, limit=10) => { for (const el of Array.from(document.querySelectorAll(q)).slice(0,limit)) picks.push(el); };
    pushQ('header img, .site-logo img, img[alt*=logo i], img[src*="logo"]', 5);
    pushQ('button, [role=button], a.button, a.btn, [class*="btn"]', 25);
    pushQ('input, select, textarea, .input, [class*="form-control"]', 25);
    pushQ('.card, [class*="card"], .panel, .tile, .box, .paper', 25);
    pushQ('h1,h2,h3,h4, p, small, code, pre', 50);
    pushQ('a', 50);
    const all = Array.from(document.querySelectorAll('body *')).filter(e => e.offsetWidth && e.offsetHeight);
    all.sort((a,b)=> (b.offsetWidth*b.offsetHeight) - (a.offsetWidth*a.offsetHeight));
    picks.push(...all.slice(0,150));
    return dedupe(picks);
  };

  const getStyleSnapshot = (el) => {
    const cs = getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    const style = (prop) => cs.getPropertyValue(prop);
    const color = hexify(style('color'));
    const bg = hexify(style('background-color'));
    const bc = hexify(style('border-top-color'));
    const fill = hexify(style('fill'));
    const stroke = hexify(style('stroke'));
    const radius = toPx(style('border-radius'));
    const fw = parseInt(style('font-weight'),10) || null;
    const ff = (style('font-family') || '').split(',')[0]?.replace(/["']/g,'').trim();
    const fs = style('font-size');
    const lhRaw = style('line-height');
    let lh = null;
    if (/\d/.test(lhRaw)) {
      const px = toPx(lhRaw);
      const fpx = toPx(fs) || 16;
      lh = px && fpx ? +(px / fpx).toFixed(2) : null;
    }
    const trDur = style('transition-duration');
    const trEase = style('transition-timing-function');
    return {
      tag: el.tagName.toLowerCase(),
      classes: Array.from(el.classList || []),
      role: el.getAttribute('role') || '',
      rect: {w: rect.width, h: rect.height},
      colors: {text: color, background: bg, border: bc, fill, stroke},
      typography: {family: ff || null, size: fs || null, weight: fw, lineHeight: lh},
      radius,
      transition: {duration: trDur || '', easing: trEase || ''},
      isButtonLike: el.matches('button,[role=button],a.button,a.btn,[class*="btn"]'),
      isInputLike: el.matches('input,select,textarea,.input,[class*="form-control"]'),
      isCardLike: el.matches('.card,[class*="card"],.panel,.tile,.box,.paper'),
      isLink: el.matches('a')
    };
  };

  const findImages = () => {
    const imgs = [];
    const push = (src, type) => { if (src) imgs.push({type, src}); };

    push(document.querySelector('link[rel*="icon" i]')?.href, 'favicon');
    push(document.querySelector('link[rel="mask-icon"]')?.href, 'mask-icon');
    push(document.querySelector('meta[property="og:image" i]')?.content, 'og');
    push(document.querySelector('meta[name="twitter:image" i]')?.content, 'twitter');
    const logoImg = Array.from(document.images).find(img =>
      /logo/i.test(img.alt || '') || /logo/i.test(img.src) || img.closest('[class*="logo"]')
    );
    if (logoImg) push(logoImg.src, 'logo');

    const svgLogo = Array.from(document.querySelectorAll('svg')).find(s => /logo/i.test(s.id) || /logo/i.test(s.className?.baseVal || ''));
    if (svgLogo) {
      const serializer = new XMLSerializer();
      const svgStr = 'data:image/svg+xml;utf8,' + encodeURIComponent(serializer.serializeToString(svgLogo));
      push(svgStr, 'logo-svg');
    }

    return imgs;
  };

  const inferBaseUnit = (values) => {
    const vs = values.filter(v => Number.isFinite(v) && v > 0 && v <= 128)
                     .map(v => Math.round(v));
    if (vs.length === 0) return 8;
    const candidates = [4,6,8,10,12];
    for (const c of candidates) {
      const ok = vs.filter(v => v % c === 0 || Math.abs(v % c - c) <= 1 || (v % c) <= 1).length / vs.length;
      if (ok >= 0.6) return c;
    }
    vs.sort((a,b)=>a-b);
    const med = vs[Math.floor(vs.length/2)];
    return Math.max(2, Math.min(12, Math.round(med/2)*2));
  };

  const inferPalette = (snapshots, cssColors) => {
    const freq = new Map();
    const bump = (hex, weight=1) => {
      if (!hex) return;
      freq.set(hex, (freq.get(hex) || 0) + weight);
    };
    for (const s of snapshots) {
      const area = Math.max(1, s.rect.w * s.rect.h);
      bump(s.colors.background, 0.5 + Math.log10(area+10));
      bump(s.colors.text, 1.0);
      bump(s.colors.border, 0.3);
      bump(s.colors.fill, 0.6);
      bump(s.colors.stroke, 0.4);
    }
    for (const c of cssColors) bump(c, 0.5);
    const ranked = Array.from(freq.entries())
      .filter(([h]) => h && !/^#000000$/i.test(h) || /^#ffffff$/i.test(h) || true)
      .sort((a,b)=> b[1]-a[1])
      .map(([h]) => h);

    const isGrayish = (hex) => {
      const h = hex.replace('#',''); if (h.length<6) return true;
      const r=parseInt(h.slice(0,2),16), g=parseInt(h.slice(2,4),16), b=parseInt(h.slice(4,6),16);
      const max = Math.max(r,g,b), min = Math.min(r,g,b);
      return (max - min) < 15;
    };

    const bgCandidate = ranked.find(h => isGrayish(h) && contrastYIQ(h) > 180) || '#FFFFFF';
    const textPrimary = ranked.find(h => !/^#FFFFFF$/i.test(h) && contrastYIQ(h) < 160) || '#111111';

    const prim = ranked.find(h => !isGrayish(h) && h !== textPrimary && h !== bgCandidate) ||
                 ranked.find(h => !/^#FFFFFF$/i.test(h) && !/^#000000$/i.test(h)) || '#000000';
    const sec = ranked.find(h => h !== prim && !isGrayish(h)) ||
                ranked.find(h => h !== prim) || '#666666';
    const accent = ranked.find(h => h !== prim && h !== sec && !isGrayish(h)) || prim;

    const link = (()=>{
      const a = document.querySelector('a');
      if (a) return hexify(getComputedStyle(a).color) || accent;
      return accent;
    })();

    return {
      primary: prim,
      secondary: sec,
      accent,
      background: bgCandidate,
      text_primary: textPrimary,
      text_secondary: '#666666',
      link,
      success: ['#22C55E','#28A745','#16A34A','#2ECC71'].find(c => cssColors.includes(c)) || '#28A745',
      warning: ['#F59E0B','#FFC107','#FACC15'].find(c => cssColors.includes(c)) || '#FFC107',
      error: ['#EF4444','#DC3545','#F87171'].find(c => cssColors.includes(c)) || '#DC3545'
    };
  };

  const inferTypography = () => {
    const pickFF = (el, fallback) => {
      const ff = getComputedStyle(el).fontFamily?.split(',')[0]?.replace(/["']/g,'').trim();
      return ff || fallback;
    };
    const h1 = document.querySelector('h1') || document.body;
    const h2 = document.querySelector('h2') || document.body;
    const h3 = document.querySelector('h3') || document.body;
    const p = document.querySelector('p') || document.body;
    const small = document.querySelector('small') || document.body;
    const code = document.querySelector('code, pre') || document.body;
    const body = document.body;

    const size = el => getComputedStyle(el).fontSize;
    const lh = el => {
      const cs = getComputedStyle(el);
      const lhRaw = cs.lineHeight;
      const px = toPx(lhRaw);
      const fpx = toPx(cs.fontSize) || 16;
      if (!px || !fpx) return 1.5;
      return +(px/fpx).toFixed(2);
    };

    return {
      font_families: {
        primary: pickFF(body, 'Inter, sans-serif'),
        secondary: pickFF(h1, pickFF(h2, 'Georgia, serif')),
        code: pickFF(code, 'Fira Code, monospace'),
      },
      font_sizes: {
        h1: size(h1) || '48px',
        h2: size(h2) || '36px',
        h3: size(h3) || '28px',
        body: size(p) || '16px',
        small: size(small) || '14px',
      },
      line_heights: {
        heading: lh(h1),
        body: lh(p)
      },
      font_weights: {
        light: 300,
        regular: 400,
        medium: 500,
        bold: 700
      }
    };
  };

  const pickBorderRadius = (snapshots) => {
    const rs = snapshots.map(s=>s.radius).filter(v=>Number.isFinite(v));
    if (!rs.length) return '8px';
    rs.sort((a,b)=>a-b);
    const med = rs[Math.floor(rs.length/2)];
    return Math.round(med) + 'px';
  };

  const buildComponents = (snapshots, palette) => {
    const btn = snapshots.find(s => s.isButtonLike) || snapshots.find(s => s.isLink);
    const input = snapshots.find(s => s.isInputLike);
    const card = snapshots.find(s => s.isCardLike);
    const defRadius = pickBorderRadius(snapshots);

    return {
      buttons: {
        primary: {
          background: btn?.colors.background || palette.accent,
          text_color: btn?.colors.text || (contrastYIQ(btn?.colors.background) < 128 ? '#FFFFFF' : '#111111'),
          hover_background: undefined,
          border_radius: defRadius
        },
        secondary: {
          background: '#FFFFFF',
          text_color: palette.accent,
          border: \`1px solid ${palette.accent}\`
        }
      },
      inputs: {
        border_color: input?.colors.border || '#CCCCCC',
        focus_border_color: palette.accent,
        border_radius: defRadius
      },
      cards: {
        background: card?.colors.background || '#FFFFFF',
        shadow: '0px 2px 8px rgba(0,0,0,0.1)',
        border_radius: defRadius
      }
    };
  };

  const pickLogo = (images) => {
    const byType = (t) => images.find(i=>i.type===t)?.src;
    return byType('logo') || byType('logo-svg') || byType('og') || byType('twitter') || byType('favicon') || null;
  };

  const inferFontsList = (fontFaces, cssFonts) => {
    const fams = dedupe(fontFaces.map(f=>f.family).concat(cssFonts)).filter(Boolean);
    return fams.map(f => ({ family: f }));
  };

  const inferAnimations = (snapshots, cssTransitions) => {
    const t = snapshots.map(s=>s.transition).concat(cssTransitions || []);
    const dur = (t.find(x => x.duration && x.duration !== '0s')?.duration) || '0.3s';
    const ease = (t.find(x => x.easing && x.easing !== 'ease')?.easing) || 'cubic-bezier(0.4, 0, 0.2, 1)';
    return { transition_duration: dur, easing: ease };
  };

  const inferLayout = (cssMaxWidths) => {
    const mw = cssMaxWidths.find(x => /\d(px|rem|em)/.test(x)) || '1280px';
    return {
      grid: { columns: 12, max_width: mw },
      header_height: (() => {
        const h = document.querySelector('header'); if (!h) return '80px';
        const r = h.getBoundingClientRect(); return Math.round(r.height) + 'px';
      })(),
      footer_height: (() => {
        const f = document.querySelector('footer'); if (!f) return '200px';
        const r = f.getBoundingClientRect(); return Math.round(r.height) + 'px';
      })()
    };
  };

  const inferTone = () => ({
    voice: 'friendly, professional, clear',
    emoji_usage: 'sparingly, only in marketing pages'
  });

  const cssData = collectSameOriginRules();
  const nodes = sampleNodes();
  const snaps = nodes.map(getStyleSnapshot);

  const palette = inferPalette(snaps, cssData.colorsInCSS);
  const typography = inferTypography();

  const spacingVals = cssData.spacings;
  const baseUnit = inferBaseUnit(spacingVals);
  const borderRadius = pickBorderRadius(snaps);

  const iconStyle = cssData.iconHints.includes('material-icons') ? 'outlined' : 'outlined';

  const images = findImages();
  const imagesOut = {
    logo: pickLogo(images),
    favicon: images.find(i=>i.type==='favicon')?.src || null,
    default_og_image: images.find(i=>i.type==='og')?.src || images.find(i=>i.type==='twitter')?.src || null
  };

  const fontsList = inferFontsList(cssData.fontFaces, cssData.fontsInCSS);

  const components = buildComponents(snaps, palette);

  const animations = inferAnimations(snaps, cssData.transitions);

  const layout = inferLayout(cssData.gridMaxWidths);

  const result = {
    logo: imagesOut.logo,
    fonts: fontsList,
    colors: palette,
    typography,
    spacing: {
      base_unit: baseUnit,
      padding: { small: baseUnit, medium: baseUnit*2, large: baseUnit*4 },
      margins: { small: baseUnit, medium: baseUnit*2, large: baseUnit*4 },
      grid_gutter: baseUnit*3,
      border_radius: borderRadius
    },
    components,
    icons: {
      style: iconStyle,
      primary_color: typography.font_families?.primary ? undefined : '#111111'
    },
    images: imagesOut,
    animations,
    layout,
    tone: inferTone()
  };

  const clean = (obj) => {
    if (Array.isArray(obj)) return obj.map(clean).filter(v => v != null && (typeof v !== 'object' || Object.keys(v).length));
    if (obj && typeof obj === 'object') {
      const o = {};
      for (const [k,v] of Object.entries(obj)) {
        const cv = clean(v);
        if (cv !== null && (typeof cv !== 'object' || (Array.isArray(cv) ? cv.length : Object.keys(cv).length))) o[k]=cv;
      }
      return o;
    }
    return obj == null ? null : obj;
  };

  return { type: 'branding', value: clean(result) };
})()`;
