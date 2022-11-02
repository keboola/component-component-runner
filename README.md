Component Runner
=============

This component runs a job of a specified component with a specified set of variables.

**Table of contents:**

[TOC]
Prerequisites
=============

Get a Keboola Limited Access SAPI Token with restricted access to the component you wish to run in the project you wish to run it in.

Configuration
=============

## Configuration Schema
 - Component parameters (component_parameters) - [REQ] 
   - KBC Storage API token (#sapi_token) - [REQ] Limited Access SAPI Token with restricted access to the component you wish to run in the project you wish to run it in.
   - KBC Stack (keboola_stack) - [OPT] The stack that your component configuration is in
   - Custom Stack (custom_stack) - [OPT] The name of your stack in connection.{CUSTOM_STACK}.keboola.cloud
   - Component ID (component_id) - [REQ] The ID of the component you wish to run. Get the component id from the link :  ...keboola.com/admin/projects/{PROJECT_ID}/{COMPONENT_ID}
   - Configuration ID (config_id) - [REQ] The ID of the configuration of a component you wish to run. Get the configuration id from the link :  ...keboola.com/admin/projects/{PROJECT_ID}/{COMPONENT_ID}/{CONFIGURATION_ID}
 - Run parameters (run_parameters) - [OPT] 
   - Wait until finish (wait_until_finish) - [OPT] If checked, will wait for the execution of the job in Keboola to be finished
   - Use variables (use_variables) - [OPT] If checked, the defined variables will be used in the job run
   - Variable mode (variable_mode) - [OPT] Select which mode of input of variables you want to use
   - Variables (variables) - [OPT] description
     - Name (name) - [REQ] Name of the variable
     - Value (value) - [REQ] Value of the variable




Sample Configuration
=============
```json
{
    "parameters": {
        "component_parameters": {
            "#sapi_token": "SECRET_VALUE",
            "keboola_stack": "",
            "component_id": "keboola.python-transformation-v2",
            "config_id": "810494893"
        },
        "run_parameters": {
            "wait_until_finish": true,
            "use_variables": true,
            "variable_mode": "self_defined",
            "variables": [
                {
                    "name": "var1",
                    "value": "hi"
                },
                {
                    "name": "var2",
                    "value": "hello"
                }
            ]
        }
    },
    "action": "run"
}
```

Development
-----------

If required, change local data folder (the `CUSTOM_FOLDER` placeholder) path to your custom path in
the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, init the workspace and run the component with following command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and lint check using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration
===========

For information about deployment and integration with KBC, please refer to the
[deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/)