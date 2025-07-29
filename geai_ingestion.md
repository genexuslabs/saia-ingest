
## Ingestion

By default all `ingestion` parameters are taken from the RAG assistant. If you want to override this mechanism add a ingestion element withing the `saia` keyword, valid parameters for the `geai` provider (default) are detailed [here](https://wiki.genexus.com/enterprise-ai/wiki?1256,geai+Ingestion+Provider+Parameters); you need to add the specific key-value elements:

Example configuration using specific configuration

```yaml
saia:
  ...
  ingestion:
    provider: !!str geai
    strategy: !!str hi_res
    model: !!str openai/gpt-4.1-nano
    structure: !!str table
    dpi: !!int 205
    ...
```

## See Also

[Ingestion Providers](https://wiki.genexus.com/enterprise-ai/wiki?581,Ingestion+Provider,)