# Logo Setup — TradeFlow NG Frontend

You have two logo files. Here is exactly what to name them and where to put them.

## Files to add → `frontend/public/`

Place these four files in your `frontend/public/` folder:

```
frontend/public/
├── logo-icon.png          ← The circular icon logo (green circle with S-flow symbol)
├── logo-full.png          ← Full horizontal logo on light/cream background
├── logo-full-white.png    ← Full horizontal logo on dark/transparent background
└── favicon.ico            ← Use the circular icon, converted to .ico
```

## Which file is which

| File name | Description | Used in |
|-----------|-------------|---------|
| `logo-icon.png` | Circular icon only (Image 2 you shared) | Login page header, sidebar, agent header |
| `logo-full.png` | "TradeFlow NG" wordmark (Image 1 — light version) | Admin login card |
| `logo-full-white.png` | Same wordmark but white text for dark backgrounds | Sidebar, agent header, dark login screen |

## Quick steps

1. Save Image 2 (the circle icon) as `logo-icon.png` → drop into `frontend/public/`
2. Save Image 1 (the full wordmark on cream) as `logo-full.png` → drop into `frontend/public/`
3. If your designer has a white-text version of the full logo, save it as `logo-full-white.png`
4. If you don't have `logo-full-white.png` yet, just duplicate `logo-full.png` for now — 
   the code has graceful fallbacks so it won't break

## Fallback behaviour

All `<img>` tags in the app have `onError` handlers. If a logo file is missing, the app will 
silently hide the broken image and fall back to a text label. So nothing will break while you 
get the right files in place.
