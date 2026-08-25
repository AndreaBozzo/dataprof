# README hero assets

The root `README.md` is also the package description on PyPI and crates.io, so
its hero image has two constraints a repository-only asset does not:

- **It must be an absolute URL.** A relative path resolves against the package
  page, where it points at nothing. The README references the hero through
  `https://raw.githubusercontent.com/AndreaBozzo/dataprof/HEAD/assets/images/logo.webp`.
- **It must be small.** Every visitor to either index downloads it.

## Files

| File | Size | Role |
| --- | --- | --- |
| `logo.webp` | 35 KB, 1600x873 | What the README loads. Referenced by absolute raw URL. |
| `logo.png` | 3.8 MB, 2816x1536 | The master. Not referenced anywhere; kept so the hero can be re-exported. |

`logo.webp` is displayed at `width="800"`, so it is stored at twice that for
high-density screens.

## Regenerating

If the master changes, re-export with the same settings:

```python
from PIL import Image

master = Image.open("assets/images/logo.png").convert("RGB")
master.resize((1600, 873), Image.LANCZOS).save(
    "assets/images/logo.webp", format="WEBP", quality=90, method=6
)
```

WebP at quality 90 measured 47.9 dB PSNR against the master downscaled to the
rendered 800px width, which is visually lossless. PNG cannot reach this size:
the artwork has ~30,000 unique colours from its gradients and textured
background, so even a palette-quantised 800px PNG is 213 KB, six times larger
and at half the resolution.

Keep the result under 250 KB. If the artwork ever gains a transparent
background, re-check it against a dark theme before committing.
