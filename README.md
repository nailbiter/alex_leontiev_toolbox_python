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

I call a class `WA` a *wrapper* for class `A`, when:

1. `WA`'s constructor accepts a single argument being member of class `A`;
2. it is implicitly understood that every instance of `WA` carries a single underlying instance of `A` (latter referred to as *kernel*; former
as *shell*);

I call a wrapper *opaque* if the only way to set a *kernel* is via constructor, and it cannot be later modified during the lifetime of shell.
Kernel *may* be provided as a public read-only member.

If the kernel can be re-set at any later point of the lifetime of shell with everything working seamlessly. In this case normally kernel should
be provided as a public read-write member.
