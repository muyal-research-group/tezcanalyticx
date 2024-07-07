
#!/bin/bash

uvicorn tezcanalyticx.server:app --host ${TEZCANALYTICX_IP_ADDR-0.0.0.0} --port ${TEZCANALYTICX_PORT-45000}
