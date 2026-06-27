# Config Lifecycle

The first runtime milestone treats config as an admission-time input. A host
loads config, applies deterministic plugins, validates the resulting snapshot,
and then freezes a clone into the run snapshot. Later reloads affect only later
run admissions.

## Order

1. `config.Loader` returns a candidate `config.Snapshot`.
2. Optional config plugins mutate that candidate in deterministic name order.
3. The lifecycle validates permissions, observability policy, agent identity,
   and model selection before runtime execution begins.
4. `runtime.FreezeTurnSnapshot` clones config and message containers for the
   provider turn.

Plugin errors are admission failures. They do not create a partially admitted
run, and they do not mutate an already frozen run snapshot.

## Reload Limits

Reloading config in this milestone does not change an in-flight run, its tool
materialization, its provider/model choice, or its hook list. It only changes
future calls to the loader/lifecycle. Plugins should therefore avoid hidden
global state and should write every run-affecting value into the snapshot they
receive.
