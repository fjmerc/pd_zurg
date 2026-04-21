# Zurgarr brand assets

| File | Purpose | Dimensions |
|---|---|---|
| `zurgarr.svg` | Master icon. Used as the WebUI favicon and the source for all raster exports. | 1024×1024 |
| `zurgarr-social.svg` | GitHub social preview / repo card image (icon + wordmark + tagline). | 1280×640 |

## Design

- **Shape:** rounded square, `rx=128` on a 1024 canvas (12.5% corner radius — matches the *arr ecosystem convention used by Sonarr / Lidarr / Prowlarr).
- **Color:** violet `#7c3aed`. Chosen because purple is unclaimed in the *arr ecosystem (Sonarr/Bazarr=blue, Radarr=yellow, Lidarr=green, Prowlarr=orange, Whisparr=pink, Tdarr=teal, Readarr=red).
- **Glyph:** bold geometric "Z" in white, ~50% of canvas, tuned to read at 16×16 favicon size.

## Regenerating raster exports

The repo ships SVG only. Modern browsers render the SVG favicon natively, so no raster favicon is needed for the WebUI. PNGs are required only for two upload destinations that don't accept SVG:

- **Docker Hub logo** — needs PNG, ≥256×256, ideally 1024×1024.
- **GitHub repo social preview** — needs PNG/JPG, 1280×640.

To regenerate them with `rsvg-convert` (Debian/Ubuntu: `apt install librsvg2-bin`):

```bash
# Docker Hub logo (1024×1024)
rsvg-convert -w 1024 -h 1024 zurgarr.svg -o zurgarr-1024.png

# GitHub social preview (1280×640)
rsvg-convert -w 1280 -h 640 zurgarr-social.svg -o zurgarr-social.png
```

With ImageMagick (`apt install imagemagick`):

```bash
convert -density 300 -background none zurgarr.svg -resize 1024x1024 zurgarr-1024.png
convert -density 300 -background none zurgarr-social.svg -resize 1280x640 zurgarr-social.png
```

With Inkscape:

```bash
inkscape zurgarr.svg --export-type=png --export-width=1024 --export-filename=zurgarr-1024.png
inkscape zurgarr-social.svg --export-type=png --export-width=1280 --export-filename=zurgarr-social.png
```

Or upload the SVG to any online SVG→PNG converter (e.g. CloudConvert) and download at the target size.

## Uploading

- **Docker Hub:** Settings → Repository Settings → upload `zurgarr-1024.png` as the icon.
- **GitHub social preview:** Settings → Social preview → upload `zurgarr-social.png`.
- **WebUI favicon:** already wired in via `utils/ui_common.py` — both the static `<link rel="icon">` and the dynamic `FAVICON_JS` health-status colour swap embed the icon as an inline `data:image/svg+xml` URI, so no extra HTTP request and no file-serving route is needed at runtime. The standalone `assets/zurgarr.svg` file is the design source of truth, kept in sync by hand if the inline path data ever changes.
