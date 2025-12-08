The Component Runner allows you to trigger and run jobs of other Keboola components with custom variables. This is useful for orchestrating complex workflows where you need to run components with different parameter sets or chain component executions together.
Allows to
Run any component: Execute any Keboola component by specifying its Component ID and Configuration ID
Variable support: Pass custom variables to the component run, either defined manually or loaded from an input table
Multiple run modes: Run with variables from a table (all rows or first row only) or use manually defined variables
Cross-stack support: Run components on any Keboola stack (AWS US, AWS EU, Azure, GCP, or custom stacks)
Wait for completion: Optionally wait for the triggered job to finish before continuing
