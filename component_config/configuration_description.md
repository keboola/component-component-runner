## Prerequisites

Before configuring the Component Runner, you need a **Limited Access SAPI Token** with restricted access to the component you wish to run in the target project.

## Configuration Steps

1. **Enter your SAPI Token**: Provide a Limited Access token with permissions for the target component
2. **Select the KBC Stack**: Choose the stack where your target component configuration exists
3. **Select the Component**: Use the dropdown to select which component to run
4. **Select the Configuration**: Choose which configuration of the component to execute

## Using Variables

You can pass variables to the component run in two ways:

**Manual Variables**: Define variable name-value pairs directly in the configuration

**Input Table Variables**: Provide an input table where column names match variable names. Each row triggers a separate component run with those variable values.

| Variable Mode | Description |
|--------------|-------------|
| Variables Defined in Configuration | Use manually entered variable name-value pairs |
| Variables Defined by Input Table - run all | Run the component once for each row in the input table |
| Variables Defined by Input Table - run only first row | Run the component only with the first row of variables |
