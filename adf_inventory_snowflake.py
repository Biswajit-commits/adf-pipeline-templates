"""
Azure Data Factory Comprehensive Inventory Agent — Snowflake Workspace (Git) edition
-----------------------------------------------------------------------------------
Same inventory and classification as ADF_Inventory_Agent.py (written as CSV +
JSON to adf_inventory_output/, standard library only), but
reads the ADF Git-integration JSON files (pipeline/, dataset/, dataflow/,
linkedService/, trigger/, integrationRuntime/, factory/, ...) from this
workspace instead of calling the Azure Management REST API. No Azure
credentials or external network access are required.

Usage: run this file from the Snowflake Workspace (Run button).
"""

import json
import os
import sys
import warnings
from datetime import datetime
import csv

def _find_repo_root() -> str:
    candidates = []
    try:
        candidates.append(os.path.dirname(os.path.abspath(__file__)))
    except NameError:
        pass
    candidates += [os.getcwd(), "/workspace"]
    for c in candidates:
        if os.path.isdir(os.path.join(c, "pipeline")):
            return c
    return candidates[0]


REPO_ROOT = _find_repo_root()

warnings.filterwarnings("ignore")

# ── Configuration ─────────────────────────────────────────────────────────────
FACTORY_NAME      = "ADFAccenLulu"   # factory/<name>.json to report on
RESOURCE_GROUP    = "N/A (from Git)"
SUBSCRIPTION_ID   = "N/A (from Git)"
GIT_REPO_NAME     = "ADF_Pipelines"
GIT_BRANCH        = "main"
OUTPUT_DIR        = os.path.join(REPO_ROOT, "adf_inventory_output")
# ─────────────────────────────────────────────────────────────────────────────

# REST collection name -> Git folder name(s)
_FOLDERS = {
    "pipelines":           ["pipelines"],
    "dataflows":           ["dataflow"],
    "datasets":            ["dataset"],
    "linkedservices":      ["linkedServices"],
    "integrationruntimes": ["integrationRuntime"],
    "triggers":            ["triggers"],
}
_cache: dict = {}


def _load_folder(folder: str) -> list[dict]:
    path = os.path.join(REPO_ROOT, folder)
    items = []
    if not os.path.isdir(path):
        return items
    for fn in sorted(os.listdir(path)):
        if not fn.lower().endswith(".json"):
            continue
        try:
            with open(os.path.join(path, fn), encoding="utf-8-sig") as fh:
                items.append(json.load(fh))
        except Exception as e:
            print(f"  [WARN] {folder}/{fn} -> {e}")
    return items


def paginate(path: str) -> list[dict]:
    """Return all items of a resource collection from the Git folders."""
    key = path.strip("/").lower()
    if key not in _cache:
        items = []
        for folder in _FOLDERS.get(key, [key]):
            items.extend(_load_folder(folder))
        _cache[key] = items
    return _cache[key]


def api_get(path: str, params: dict = None) -> dict:
    """Only '/' (factory resource) is used; built from factory/<FACTORY_NAME>.json."""
    fp = os.path.join(REPO_ROOT, "factory", f"{FACTORY_NAME}.json")
    data = {}
    if os.path.exists(fp):
        with open(fp, encoding="utf-8-sig") as fh:
            data = json.load(fh)
    props = data.setdefault("properties", {})
    # Git factory files don't carry repo config / state; fill from known Git context.
    props.setdefault("provisioningState", "N/A (from Git)")
    props.setdefault("repoConfiguration", {
        "type": "Git (workspace)",
        "repositoryName": GIT_REPO_NAME,
        "collaborationBranch": GIT_BRANCH,
    })
    return data


# ── Helpers ───────────────────────────────────────────────────────────────────
def _safe(v, fallback="N/A"):
    if v is None or v == "":
        return fallback
    return v


def _ts(iso: str) -> str:
    if not iso:
        return "N/A"
    try:
        return iso.replace("T", " ").split(".")[0].split("Z")[0]
    except Exception:
        return str(iso)


def _props(item: dict) -> dict:
    return item.get("properties", {})


def _str_list(lst) -> str:
    if not lst:
        return ""
    return ", ".join(str(x) for x in lst)


def _count(v) -> int:
    if isinstance(v, list):
        return len(v)
    if isinstance(v, dict):
        return len(v)
    return 0


# ── Resource Fetchers ─────────────────────────────────────────────────────────

def get_factory_info() -> list[dict]:
    """Factory-level metadata (single row)."""
    data = api_get("/") or {}
    p    = _props(data)
    gi   = p.get("globalParameters", {})
    rows = [{
        "Factory Name":         data.get("name", FACTORY_NAME),
        "Resource Group":       RESOURCE_GROUP,
        "Subscription ID":      SUBSCRIPTION_ID,
        "Location":             _safe(data.get("location")),
        "State":                _safe(p.get("provisioningState")),
        "Create Time":          _ts(p.get("createTime")),
        "Purview Account":      _safe(p.get("purviewConfiguration", {}).get("purviewResourceId", "N/A")),
        "Git Config Enabled":   "Yes" if p.get("repoConfiguration") else "No",
        "Repo Type":            _safe(p.get("repoConfiguration", {}).get("type", "N/A")),
        "Repo Name":            _safe(p.get("repoConfiguration", {}).get("repositoryName", "N/A")),
        "Branch":               _safe(p.get("repoConfiguration", {}).get("collaborationBranch", "N/A")),
        "Global Parameters":    _count(gi),
        "Public Network Access":_safe(p.get("publicNetworkAccess", "Enabled")),
        "Tags":                 str(data.get("tags", {})),
    }]
    return rows


def get_pipelines() -> list[dict]:
    items = paginate("pipelines")
    rows  = []
    for item in items:
        p           = _props(item)
        activities  = p.get("activities", [])
        params      = p.get("parameters", {})
        variables   = p.get("variables", {})
        folders     = p.get("folder", {})
        rows.append({
            "Pipeline Name":      item.get("name"),
            "Description":        _safe(p.get("description", ""), ""),
            "Folder":             _safe(folders.get("name", ""), "Root"),
            "Activity Count":     len(activities),
            "Activity Types":     _str_list(sorted({a.get("type","") for a in activities})),
            "Parameter Count":    _count(params),
            "Variable Count":     _count(variables),
            "Concurrency":        _safe(p.get("concurrency")),
            "Annotations":        _str_list(p.get("annotations", [])),
            "Last Publish Time":  _ts(item.get("etag", "")),
        })
    return rows


def get_activities() -> list[dict]:
    """Flatten all activities from all pipelines."""
    items = paginate("pipelines")
    rows  = []
    for item in items:
        pipeline_name = item.get("name", "")
        p             = _props(item)
        for act in p.get("activities", []):
            dep_on = act.get("dependsOn", [])
            depends = _str_list([d.get("activity", "") for d in dep_on])

            # Extract linked service ref if present
            ls_ref = ""
            for key in ("linkedServiceName", "linkedService"):
                ls = act.get(key, {})
                if isinstance(ls, dict):
                    ls_ref = ls.get("referenceName", "")
                    if ls_ref:
                        break

            # Extract dataset refs
            inputs  = act.get("inputs",  [])
            outputs = act.get("outputs", [])
            in_ds   = _str_list([i.get("referenceName", "") for i in inputs])
            out_ds  = _str_list([o.get("referenceName", "") for o in outputs])

            # Type-specific notes
            type_props = act.get("typeProperties", {})
            notes = []
            if "url" in type_props:
                notes.append(f"url={str(type_props['url'])[:60]}")
            if "pipeline" in type_props:
                notes.append(f"calls={type_props['pipeline'].get('referenceName','')}")
            if "dataflow" in type_props:
                notes.append(f"dataflow={type_props['dataflow'].get('referenceName','')}")
            if "waitTimeInSeconds" in type_props:
                notes.append(f"wait={type_props['waitTimeInSeconds']}s")
            if "expression" in type_props:
                expr = type_props["expression"]
                if isinstance(expr, dict):
                    notes.append(f"expr={str(expr.get('value',''))[:60]}")

            rows.append({
                "Pipeline Name":    pipeline_name,
                "Activity Name":    act.get("name", ""),
                "Activity Type":    act.get("type", ""),
                "Description":      _safe(act.get("description", ""), ""),
                "Depends On":       depends,
                "Input Datasets":   in_ds,
                "Output Datasets":  out_ds,
                "Linked Service":   ls_ref,
                "Notes":            "; ".join(notes),
                "Policy Timeout":   _safe(act.get("policy", {}).get("timeout", ""), ""),
                "Policy Retries":   _safe(act.get("policy", {}).get("retry", ""), ""),
            })
    return rows


def get_dataflows() -> list[dict]:
    items = paginate("dataflows")
    rows  = []
    for item in items:
        p    = _props(item)
        kind = p.get("type", "MappingDataFlow")
        tf   = p.get("typeProperties", {})
        sources    = tf.get("sources", [])
        sinks      = tf.get("sinks", [])
        transforms = tf.get("transformations", [])
        script     = tf.get("script", "") or ""
        rows.append({
            "DataFlow Name":      item.get("name"),
            "Type":               kind,
            "Description":        _safe(p.get("description", ""), ""),
            "Folder":             _safe(p.get("folder", {}).get("name", ""), "Root"),
            "Source Count":       len(sources),
            "Sources":            _str_list([s.get("name","") for s in sources]),
            "Sink Count":         len(sinks),
            "Sinks":              _str_list([s.get("name","") for s in sinks]),
            "Transformation Count": len(transforms),
            "Transformations":    _str_list([t.get("name","") for t in transforms]),
            "Script Lines":       len(script.splitlines()) if script else 0,
            "Annotations":        _str_list(p.get("annotations", [])),
        })
    return rows


def get_flowlets() -> list[dict]:
    """Flowlets share the /dataflows endpoint but have type=Flowlet."""
    items = paginate("dataflows")
    rows  = []
    for item in items:
        p    = _props(item)
        kind = p.get("type", "")
        if kind.lower() != "flowlet":
            continue
        tf      = p.get("typeProperties", {})
        sources = tf.get("sources", [])
        sinks   = tf.get("sinks", [])
        script  = tf.get("script", "") or ""
        rows.append({
            "Flowlet Name":       item.get("name"),
            "Description":        _safe(p.get("description", ""), ""),
            "Folder":             _safe(p.get("folder", {}).get("name", ""), "Root"),
            "Source Count":       len(sources),
            "Sources":            _str_list([s.get("name","") for s in sources]),
            "Sink Count":         len(sinks),
            "Sinks":              _str_list([s.get("name","") for s in sinks]),
            "Script Lines":       len(script.splitlines()) if script else 0,
            "Annotations":        _str_list(p.get("annotations", [])),
        })
    return rows or [{"Note": "No Flowlets found in this factory."}]


def get_datasets() -> list[dict]:
    items = paginate("datasets")
    rows  = []
    for item in items:
        p    = _props(item)
        ls   = p.get("linkedServiceName", {})
        tp   = p.get("typeProperties", {})
        schema = p.get("schema", [])
        params = p.get("parameters", {})

        # Extract location / table / path hints
        loc   = tp.get("location", {})
        hints = []
        for fld in ("tableName", "table", "fileName", "container",
                    "relativeUrl", "fileSystem", "bucketName"):
            val = tp.get(fld) or loc.get(fld)
            if val and isinstance(val, str):
                hints.append(f"{fld}={val[:50]}")

        rows.append({
            "Dataset Name":        item.get("name"),
            "Dataset Type":        p.get("type", ""),
            "Description":         _safe(p.get("description", ""), ""),
            "Folder":              _safe(p.get("folder", {}).get("name", ""), "Root"),
            "Linked Service":      _safe(ls.get("referenceName", "")),
            "Linked Service Type": _safe(ls.get("type", "")),
            "Schema Column Count": len(schema),
            "Parameter Count":     _count(params),
            "Location Type":       _safe(loc.get("type", "")),
            "Location Hints":      "; ".join(hints),
            "Annotations":         _str_list(p.get("annotations", [])),
        })
    return rows


def get_linked_services() -> list[dict]:
    items = paginate("linkedservices")
    rows  = []
    for item in items:
        p    = _props(item)
        tp   = p.get("typeProperties", {})
        ir   = p.get("connectVia", {})
        params = p.get("parameters", {})

        # Redact secrets — capture only non-secret config fields
        safe_fields = []
        secret_fields = ("password", "servicePrincipalKey", "accountKey",
                         "sasToken", "connectionString", "clientSecret",
                         "credential", "encryptedCredential")
        for k, v in tp.items():
            if any(s in k.lower() for s in secret_fields):
                safe_fields.append(f"{k}=[REDACTED]")
            elif isinstance(v, (str, int, float, bool)):
                safe_fields.append(f"{k}={str(v)[:60]}")

        rows.append({
            "Linked Service Name":    item.get("name"),
            "Type":                   p.get("type", ""),
            "Description":            _safe(p.get("description", ""), ""),
            "Integration Runtime":    _safe(ir.get("referenceName", "AutoResolve")),
            "Parameter Count":        _count(params),
            "Connection Properties":  "; ".join(safe_fields[:6]),
            "Annotations":            _str_list(p.get("annotations", [])),
        })
    return rows


def get_integration_runtimes() -> list[dict]:
    items = paginate("integrationruntimes")
    rows  = []
    for item in items:
        p    = _props(item)
        tp   = p.get("typeProperties", {})
        kind = p.get("type", "")
        rows.append({
            "IR Name":                   item.get("name"),
            "Type":                      kind,
            "Description":               _safe(p.get("description", ""), ""),
            "Location":                  _safe(tp.get("location", "AutoResolve")),
            "Node Size":                 _safe(tp.get("nodeSize", "")),
            "Node Count":                _safe(tp.get("nodeCount", "")),
            "Max Parallel Exec/Node":    _safe(tp.get("maxParallelExecutionsPerNode", "")),
            "Compute Type":              _safe(tp.get("computeProperties", {}).get("location", "")),
            "Data Flow Compute":         _safe(tp.get("dataFlowProperties", {}).get("computeType", "")),
            "VNet Enabled":              "Yes" if tp.get("customerVirtualNetwork") else "No",
            "VNet Subnet":               _safe(tp.get("customerVirtualNetwork", {}).get("subnetId", "N/A")),
            "Edition":                   _safe(tp.get("edition", "")),
            "License Type":              _safe(tp.get("licenseType", "")),
            "Catalog DB":                _safe(tp.get("ssisProperties", {})
                                               .get("catalogInfo", {})
                                               .get("catalogServerEndpoint", "N/A")),
            "Auto Update":               _safe(tp.get("autoUpdate", "")),
            "Linked IR Name":            _safe(tp.get("linkedInfo", {}).get("authorizationType", "")),
        })
    return rows


def get_triggers() -> list[dict]:
    items = paginate("triggers")
    rows  = []
    for item in items:
        p    = _props(item)
        tp   = p.get("typeProperties", {})
        kind = p.get("type", "")
        pipelines_refs = p.get("pipelines", [])

        # Schedule / recurrence
        recurrence  = tp.get("recurrence", {})
        schedule    = ""
        if recurrence:
            freq     = recurrence.get("frequency", "")
            interval = recurrence.get("interval", "")
            schedule = f"Every {interval} {freq}" if freq else ""

        # Tumbling window / Event
        if kind == "TumblingWindowTrigger":
            freq     = tp.get("frequency", "")
            interval = tp.get("interval", "")
            schedule = f"TumblingWindow: every {interval} {freq}"
        if kind == "BlobEventsTrigger":
            schedule = f"Event: {tp.get('events', [])}"
        if kind == "CustomEventsTrigger":
            schedule = f"Custom event: {tp.get('subject', '')}"

        rows.append({
            "Trigger Name":       item.get("name"),
            "Type":               kind,
            "Description":        _safe(p.get("description", ""), ""),
            "Runtime State":      _safe(p.get("runtimeState", "")),
            "Schedule":           schedule,
            "Start Time":         _ts(tp.get("startTime") or recurrence.get("startTime","")),
            "End Time":           _ts(tp.get("endTime") or recurrence.get("endTime","")),
            "Timezone":           _safe(tp.get("timeZone") or recurrence.get("timeZone","")),
            "Pipeline Count":     len(pipelines_refs),
            "Pipelines":          _str_list([pp.get("pipelineReference",{})
                                             .get("referenceName","") for pp in pipelines_refs]),
            "Annotations":        _str_list(p.get("annotations", [])),
        })
    return rows


def get_parameters() -> list[dict]:
    """Factory-level global parameters + pipeline-level parameters."""
    rows = []

    # ── Factory global parameters ─────────────────────────────────────────────
    factory = api_get("/") or {}
    global_params = _props(factory).get("globalParameters", {})
    for name, meta in global_params.items():
        rows.append({
            "Scope":         "Factory (Global)",
            "Pipeline Name": FACTORY_NAME,
            "Parameter Name": name,
            "Type":          _safe(meta.get("type", "")),
            "Default Value": _safe(str(meta.get("value", "")), ""),
            "Description":   "",
        })

    # ── Pipeline-level parameters ─────────────────────────────────────────────
    pipelines = paginate("pipelines")
    for pl in pipelines:
        pl_name = pl.get("name", "")
        params  = _props(pl).get("parameters", {})
        for pname, pmeta in params.items():
            rows.append({
                "Scope":          "Pipeline",
                "Pipeline Name":  pl_name,
                "Parameter Name": pname,
                "Type":           _safe(pmeta.get("type", "")),
                "Default Value":  _safe(str(pmeta.get("defaultValue", "")), ""),
                "Description":    _safe(pmeta.get("description", ""), ""),
            })

    return rows or [{"Note": "No parameters found."}]


def get_variables() -> list[dict]:
    """Pipeline-level variables (ADF doesn't have factory-level variables)."""
    rows     = []
    pipelines = paginate("pipelines")
    for pl in pipelines:
        pl_name   = pl.get("name", "")
        variables = _props(pl).get("variables", {})
        for vname, vmeta in variables.items():
            rows.append({
                "Pipeline Name":  pl_name,
                "Variable Name":  vname,
                "Type":           _safe(vmeta.get("type", "")),
                "Default Value":  _safe(str(vmeta.get("defaultValue", "")), ""),
            })
    return rows or [{"Note": "No pipeline variables found."}]


# ── Complexity Classification ─────────────────────────────────────────────────

COMPLEXITY_COLORS = {
    "SIMPLE":  "70AD47",
    "MEDIUM":  "FFB900",
    "COMPLEX": "C00000",
}
COMPLEXITY_ORDER = {"SIMPLE": 0, "MEDIUM": 1, "COMPLEX": 2}


def _max_c(*levels: str) -> str:
    return max(levels, key=lambda x: COMPLEXITY_ORDER.get(x, 0))


def classify_pipeline(row: dict) -> tuple[str, str]:
    act  = int(row.get("Activity Count") or 0)
    p    = int(row.get("Parameter Count") or 0)
    v    = int(row.get("Variable Count") or 0)
    conc = row.get("Concurrency")

    reasons = []
    level   = "SIMPLE"
    if act >= 15:
        level = _max_c(level, "COMPLEX"); reasons.append(f"{act} activities (>=15)")
    if act >= 5 and level != "COMPLEX":
        level = _max_c(level, "MEDIUM"); reasons.append(f"{act} activities (5-14)")
    if p >= 5:
        level = _max_c(level, "MEDIUM"); reasons.append(f"{p} parameters")
    if v >= 5:
        level = _max_c(level, "MEDIUM"); reasons.append(f"{v} variables")
    if conc and str(conc) not in ("N/A", ""):
        level = _max_c(level, "MEDIUM"); reasons.append("concurrency set")
    return level, "; ".join(reasons) or "simple linear pipeline"


def classify_activity(row: dict) -> tuple[str, str]:
    atype   = str(row.get("Activity Type") or "").lower()
    depends = str(row.get("Depends On") or "")
    retries = row.get("Policy Retries")
    timeout = row.get("Policy Timeout")

    dep_count = len([d for d in depends.split(",") if d.strip()])
    complex_types = ("executepipeline", "executedataflow", "hdinsight",
                     "databricks", "custom", "azurefunction",
                     "webactivity", "script")
    medium_types  = ("lookup", "getmetadata", "filter", "foreach",
                     "until", "ifcondition", "switch", "wait", "setVariable")

    reasons = []
    level   = "SIMPLE"
    if any(t in atype for t in complex_types):
        level = _max_c(level, "COMPLEX"); reasons.append(f"type: {atype}")
    if dep_count >= 3:
        level = _max_c(level, "COMPLEX"); reasons.append(f"{dep_count} dependencies")
    if level != "COMPLEX":
        if any(t in atype for t in [m.lower() for m in medium_types]):
            level = _max_c(level, "MEDIUM"); reasons.append(f"type: {atype}")
        if dep_count in (1, 2):
            level = _max_c(level, "MEDIUM"); reasons.append(f"{dep_count} dependency")
        if retries and str(retries) not in ("N/A", "", "0"):
            level = _max_c(level, "MEDIUM"); reasons.append(f"retries={retries}")
        if timeout and str(timeout) not in ("N/A", "", "7.00:00:00"):
            level = _max_c(level, "MEDIUM"); reasons.append("custom timeout")
    return level, "; ".join(reasons) or "simple copy/data activity"


def classify_dataflow(row: dict) -> tuple[str, str]:
    sources = int(row.get("Source Count") or 0)
    sinks   = int(row.get("Sink Count") or 0)
    trans   = int(row.get("Transformation Count") or 0)
    lines   = int(row.get("Script Lines") or 0)

    reasons = []
    level   = "SIMPLE"
    if trans >= 10 or lines >= 200:
        level = _max_c(level, "COMPLEX"); reasons.append(f"{trans} transforms, {lines} script lines")
    if sources + sinks >= 5:
        level = _max_c(level, "COMPLEX"); reasons.append(f"{sources} sources, {sinks} sinks")
    if level != "COMPLEX":
        if trans >= 4:
            level = _max_c(level, "MEDIUM"); reasons.append(f"{trans} transformations")
        if sources + sinks in (3, 4):
            level = _max_c(level, "MEDIUM"); reasons.append(f"{sources} sources, {sinks} sinks")
    return level, "; ".join(reasons) or "simple 1-source 1-sink data flow"


def classify_dataset(row: dict) -> tuple[str, str]:
    dtype   = str(row.get("Dataset Type") or "").lower()
    schema  = int(row.get("Schema Column Count") or 0)
    params  = int(row.get("Parameter Count") or 0)

    complex_ds = ("delta", "orc", "avro", "parquet", "binary", "xml")
    medium_ds  = ("json", "excel", "snowflake", "googlesheetsdataset")

    reasons = []
    level   = "SIMPLE"
    if any(t in dtype for t in complex_ds):
        level = _max_c(level, "COMPLEX"); reasons.append(f"type: {dtype}")
    if schema >= 30:
        level = _max_c(level, "COMPLEX"); reasons.append(f"{schema} schema columns")
    if level != "COMPLEX":
        if any(t in dtype for t in medium_ds):
            level = _max_c(level, "MEDIUM"); reasons.append(f"type: {dtype}")
        if schema >= 10:
            level = _max_c(level, "MEDIUM"); reasons.append(f"{schema} schema columns")
        if params >= 2:
            level = _max_c(level, "MEDIUM"); reasons.append(f"{params} parameters")
    return level, "; ".join(reasons) or "simple flat dataset"


def classify_linked_service(row: dict) -> tuple[str, str]:
    ltype = str(row.get("Type") or "").lower()
    ir    = str(row.get("Integration Runtime") or "").lower()

    complex_ls = ("snowflake", "databricks", "hdinsight", "googlebigquery",
                  "amazonredshift", "sap", "oracle", "teradata",
                  "cosmosdb", "dynamics", "salesforce")
    medium_ls  = ("sqlserver", "azuresql", "mysql", "postgresql",
                  "restservice", "web", "odata", "azuredatalake",
                  "azuredatabricks", "azurefileshare")

    reasons = []
    level   = "SIMPLE"
    if any(t in ltype for t in complex_ls):
        level = _max_c(level, "COMPLEX"); reasons.append(f"type: {ltype}")
    if "selfhosted" in ir:
        level = _max_c(level, "COMPLEX"); reasons.append("Self-Hosted IR")
    if level != "COMPLEX":
        if any(t in ltype for t in medium_ls):
            level = _max_c(level, "MEDIUM"); reasons.append(f"type: {ltype}")
    return level, "; ".join(reasons) or "simple cloud linked service"


def classify_ir(row: dict) -> tuple[str, str]:
    ir_type = str(row.get("Type") or "").lower()
    vnet    = row.get("VNet Enabled", "No")
    nodes   = row.get("Node Count")
    catalog = str(row.get("Catalog DB") or "")

    reasons = []
    level   = "SIMPLE"
    if "selfhosted" in ir_type:
        level = _max_c(level, "COMPLEX"); reasons.append("Self-Hosted IR")
    if catalog not in ("", "N/A"):
        level = _max_c(level, "COMPLEX"); reasons.append("SSIS catalog configured")
    if level != "COMPLEX":
        if vnet == "Yes":
            level = _max_c(level, "MEDIUM"); reasons.append("VNet-managed IR")
        if nodes and str(nodes) not in ("", "N/A") and int(str(nodes)) > 1:
            level = _max_c(level, "MEDIUM"); reasons.append(f"{nodes} nodes")
    return level, "; ".join(reasons) or "auto-resolve managed IR"


def classify_trigger(row: dict) -> tuple[str, str]:
    kind     = str(row.get("Type") or "").lower()
    pl_count = int(row.get("Pipeline Count") or 0)
    state    = str(row.get("Runtime State") or "").lower()

    reasons = []
    level   = "SIMPLE"
    if "customevent" in kind or "blobevent" in kind:
        level = _max_c(level, "COMPLEX"); reasons.append(f"event-based trigger: {kind}")
    if pl_count >= 3:
        level = _max_c(level, "COMPLEX"); reasons.append(f"triggers {pl_count} pipelines")
    if level != "COMPLEX":
        if "tumbling" in kind:
            level = _max_c(level, "MEDIUM"); reasons.append("tumbling window trigger")
        if pl_count == 2:
            level = _max_c(level, "MEDIUM"); reasons.append("triggers 2 pipelines")
        if state == "stopped":
            level = _max_c(level, "MEDIUM"); reasons.append("trigger is stopped")
    return level, "; ".join(reasons) or "simple schedule trigger"


CLASSIFIERS = {
    "Pipelines":             classify_pipeline,
    "Activities":            classify_activity,
    "Data Flows":            classify_dataflow,
    "Datasets":              classify_dataset,
    "Linked Services":       classify_linked_service,
    "Integration Runtimes":  classify_ir,
    "Triggers":              classify_trigger,
}


def apply_classification(resource_type: str, rows: list[dict]) -> list[dict]:
    classifier = CLASSIFIERS.get(resource_type)
    if not classifier:
        return rows
    enriched = []
    for row in rows:
        if "Error" in row or "Note" in row:
            enriched.append(row); continue
        try:
            level, reason = classifier(row)
        except Exception as exc:
            level, reason = "SIMPLE", f"error: {exc}"
        new_row = {"Complexity": level, "Complexity Reason": reason}
        new_row.update(row)
        enriched.append(new_row)
    return enriched


# ── Resource registry ─────────────────────────────────────────────────────────

RESOURCE_REGISTRY = [
    ("Factory Info",        get_factory_info,        "0078D4"),
    ("Pipelines",           get_pipelines,            "005A9E"),
    ("Activities",          get_activities,           "003F6E"),
    ("Data Flows",          get_dataflows,            "E81123"),
    ("Flowlets",            get_flowlets,             "FF8C00"),
    ("Datasets",            get_datasets,             "107C10"),
    ("Linked Services",     get_linked_services,      "B4009E"),
    ("Integration Runtimes",get_integration_runtimes, "004E8C"),
    ("Triggers",            get_triggers,             "D83B01"),
    ("Parameters",          get_parameters,           "498205"),
    ("Variables",           get_variables,            "038387"),
]


# ── Main ──────────────────────────────────────────────────────────────────────

def _write_csv(path: str, rows: list[dict]):
    headers = []
    for r in rows:
        for k in r:
            if k not in headers:
                headers.append(k)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=headers or ["Note"])
        w.writeheader()
        w.writerows(rows)


def main():
    print("=" * 62)
    print("  Azure Data Factory Inventory Agent (CSV/JSON)")
    print(f"  Factory : {FACTORY_NAME}")
    print(f"  Source  : {REPO_ROOT}")
    print("=" * 62)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    summary, all_classified, inventory = [], [], {}
    for name, fetcher, _ in RESOURCE_REGISTRY:
        try:
            rows = fetcher()
            count = len(rows)
        except Exception as e:
            rows, count = [{"Error": str(e)}], 0
        rows = apply_classification(name, rows)
        print(f"  {name:<22} {count} record(s)")
        inventory[name] = rows
        _write_csv(os.path.join(OUTPUT_DIR, name.lower().replace(" ", "_") + ".csv"), rows)
        summary.append({
            "Resource Type": name,
            "Count":   count,
            "Simple":  sum(r.get("Complexity") == "SIMPLE" for r in rows),
            "Medium":  sum(r.get("Complexity") == "MEDIUM" for r in rows),
            "Complex": sum(r.get("Complexity") == "COMPLEX" for r in rows),
        })
        for r in rows:
            if "Complexity" in r:
                all_classified.append({
                    "Resource Type": name,
                    "Name / ID": (
                        r.get("Activity Name") or r.get("DataFlow Name") or
                        r.get("Dataset Name") or r.get("Linked Service Name") or
                        r.get("IR Name") or r.get("Trigger Name") or
                        r.get("Pipeline Name") or ""
                    ),
                    "Complexity":        r["Complexity"],
                    "Complexity Reason": r.get("Complexity Reason", ""),
                })

    _write_csv(os.path.join(OUTPUT_DIR, "classification_overview.csv"), all_classified)
    _write_csv(os.path.join(OUTPUT_DIR, "summary.csv"), summary)
    inventory["Summary"] = summary
    inventory["Classification Overview"] = all_classified
    with open(os.path.join(OUTPUT_DIR, "inventory.json"), "w", encoding="utf-8") as fh:
        json.dump(inventory, fh, indent=2, default=str)

    print(f"\n  Output written to {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
