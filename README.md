# alex_leontiev_toolbox_python
Alex Leontiev's misc Python toolbox

## todo

## install

`git+https://github.com/nailbiter/alex_leontiev_toolbox_python@main-poetry`

## test

```sh
poetry run pytest tests/test___caching.py
```

## theory

### "opaque" vs "transparent" wrappers

I call a class `WA` a **wrapper** for class `A`, when:

1. `WA`'s constructor accepts a single argument being member of class `A`

I call a wrapper 
