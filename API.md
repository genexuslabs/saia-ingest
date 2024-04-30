## API

If you need to interact with [GeneXus Enterprise AI](EnterpriseAISuite.md) APIs from python, this section is a good starting point.

The API definition is detailed [here](https://wiki.genexus.com/enterprise-ai/wiki?20,GeneXus+Enterprise+AI+API+Reference). To get access the minimal information needed are the variables:

 * $BASE_URL
 * $SAIA_APITOKEN

then, depending on the required API, extra parameters will be needed.

This library uses several of the APIs so you can check the following sources

 * [Assistants](./saia_ingest/assistant_utils.py)
 * [RAGs, Documents](./saia_ingest/profile_utils.py)
 * [Tests Folder](./tests/)
