#!/usr/bin/env python3
"""Reusable Azure Data Factory Git repository inventory framework.

Standard-library only. Auto-discovers ADF roots and resources, recursively walks
nested activities, preserves raw JSON, reports validation gaps, and writes CSV/JSON.

Usage:
  python adf_inventory_framework.py
  python adf_inventory_framework.py --repo /path/to/repo --output inventory_output
  python adf_inventory_framework.py --config inventory_config.json
"""
from __future__ import annotations
import argparse, csv, hashlib, json, os, re, sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

VERSION = "2.0.0"
CANONICAL_FOLDERS = {
    "factory": {"factory"}, "pipeline": {"pipeline", "pipelines"},
    "dataset": {"dataset", "datasets"}, "dataflow": {"dataflow", "dataflows"},
    "linked_service": {"linkedservice", "linkedservices", "linked_service"},
    "integration_runtime": {"integrationruntime", "integrationruntimes", "integration_runtime"},
    "trigger": {"trigger", "triggers"}, "credential": {"credential", "credentials"},
    "managed_virtual_network": {"managedvirtualnetwork", "managedvirtualnetworks"},
    "managed_private_endpoint": {"managedprivateendpoint", "managedprivateendpoints"},
}
SKIP_DIRS = {".git", ".github", ".idea", ".vscode", "node_modules", "adf_inventory_output", "inventory_output", "_vendor"}
DEFAULT_CONFIG = {
    "pipeline": {"medium_activities": 5, "complex_activities": 15, "medium_parameters": 5, "medium_variables": 5},
    "activity": {"complex_dependencies": 3, "complex_types": ["executepipeline","executedataflow","databricks","hdinsight","custom","azurefunction","webactivity","script"], "medium_types": ["lookup","getmetadata","filter","foreach","until","ifcondition","switch","wait","setvariable"]},
    "dataflow": {"medium_transformations": 4, "complex_transformations": 10, "complex_script_lines": 200},
    "dataset": {"medium_schema_columns": 10, "complex_schema_columns": 30},
}

@dataclass
class Issue:
    severity: str; code: str; file: str; resource_type: str = ""; message: str = ""; included: bool = False
    def row(self): return self.__dict__.copy()

@dataclass
class Resource:
    kind: str; name: str; source_file: str; data: dict[str, Any]; root: str
    @property
    def props(self): return self.data.get("properties", {}) if isinstance(self.data.get("properties", {}), dict) else {}

@dataclass
class Context:
    repo_root: Path; output_dir: Path; config: dict[str, Any]
    issues: list[Issue] = field(default_factory=list)
    resources: list[Resource] = field(default_factory=list)


def norm(s: str) -> str: return re.sub(r"[^a-z0-9]", "", s.lower())
def safe(v: Any, fallback="") -> Any: return fallback if v is None or v == "" else v
def flat(v: Any, limit=500) -> str:
    if v is None: return ""
    if isinstance(v, (dict, list)): s = json.dumps(v, ensure_ascii=False, default=str)
    else: s = str(v)
    return s[:limit]
def ref_name(v: Any) -> str: return v.get("referenceName", "") if isinstance(v, dict) else ""
def join_refs(values: Any) -> str:
    if not isinstance(values, list): return ""
    return ", ".join(filter(None, (ref_name(x) if isinstance(x, dict) else str(x) for x in values)))
def count(v: Any) -> int: return len(v) if isinstance(v, (list, dict)) else 0

def load_config(path: str | None) -> dict[str, Any]:
    cfg = json.loads(json.dumps(DEFAULT_CONFIG))
    if path:
        with open(path, encoding="utf-8-sig") as f: incoming = json.load(f)
        for k, v in incoming.items():
            cfg[k] = {**cfg.get(k, {}), **v} if isinstance(v, dict) else v
    return cfg

def resource_kind_from_folder(name: str) -> str | None:
    n = norm(name)
    for kind, aliases in CANONICAL_FOLDERS.items():
        if n in {norm(a) for a in aliases}: return kind
    return None

def score_root(path: Path) -> int:
    try: dirs = [p.name for p in path.iterdir() if p.is_dir()]
    except OSError: return 0
    return sum(1 for d in dirs if resource_kind_from_folder(d))

def discover_roots(start: Path, max_depth=4) -> list[Path]:
    start = start.resolve(); candidates=[]
    for current, dirs, _ in os.walk(start):
        p=Path(current); depth=len(p.relative_to(start).parts)
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS and depth < max_depth]
        s=score_root(p)
        if s >= 2 or (s == 1 and any(resource_kind_from_folder(d)=="factory" for d in dirs)): candidates.append((s,p))
    if not candidates and score_root(start): candidates=[(score_root(start),start)]
    candidates.sort(key=lambda x:(-x[0],len(x[1].parts)))
    selected=[]
    for _, p in candidates:
        if not any(p.is_relative_to(x) or x.is_relative_to(p) for x in selected): selected.append(p)
    return selected or [start]

def ingest(ctx: Context, roots: list[Path]) -> None:
    for root in roots:
        for folder in root.iterdir() if root.exists() else []:
            if not folder.is_dir(): continue
            kind=resource_kind_from_folder(folder.name)
            if not kind: continue
            for fp in sorted(folder.rglob("*.json")):
                rel=str(fp.relative_to(ctx.repo_root))
                try:
                    with open(fp, encoding="utf-8-sig") as f: data=json.load(f)
                    if not isinstance(data, dict): raise ValueError("top-level JSON is not an object")
                    name=str(data.get("name") or fp.stem)
                    ctx.resources.append(Resource(kind,name,rel,data,str(root.relative_to(ctx.repo_root))))
                except Exception as e:
                    ctx.issues.append(Issue("ERROR","JSON_PARSE",rel,kind,str(e),False))
    if not ctx.resources:
        ctx.issues.append(Issue("ERROR","NO_RESOURCES",str(ctx.repo_root),"","No recognized ADF JSON resources found",False))

def nested_groups(act: dict[str,Any]) -> Iterable[tuple[str,list]]:
    tp=act.get("typeProperties", {}) if isinstance(act.get("typeProperties",{}),dict) else {}
    for key in ("activities","ifTrueActivities","ifFalseActivities","defaultActivities"):
        if isinstance(tp.get(key),list): yield key,tp[key]
    for case in tp.get("cases",[]) if isinstance(tp.get("cases",[]),list) else []:
        if isinstance(case,dict) and isinstance(case.get("activities"),list): yield f"case:{case.get('value','')}",case["activities"]

def walk_activities(
    activities: Any,
    path_prefix: str = "",
    depth: int = 0,
    parent_activity: str = "",
) -> Iterable[tuple[dict, str, int, str]]:

    if not isinstance(activities, list):
        return

    for index, activity in enumerate(activities):
        if not isinstance(activity, dict):
            continue

        activity_name = str(
            activity.get("name")
            or f"activity_{index + 1}"
        )

        activity_path = (
            f"{path_prefix}/{activity_name}"
            if path_prefix
            else activity_name
        )

        yield (
            activity,
            activity_path,
            depth,
            parent_activity,
        )

        for group_label, nested_activities in nested_groups(activity):
            nested_prefix = (
                f"{activity_path}[{group_label}]"
            )

            yield from walk_activities(
                nested_activities,
                nested_prefix,
                depth + 1,
                activity_path,
            )


def classify(kind: str, row: dict[str,Any], cfg: dict[str,Any]) -> tuple[str,str,int]:
    score=0; reasons=[]
    if kind=="pipeline":
        c=cfg["pipeline"]; a=int(row.get("Activity Count",0)); p=int(row.get("Parameter Count",0)); v=int(row.get("Variable Count",0))
        if a>=c["complex_activities"]: score+=4; reasons.append(f"{a} activities")
        elif a>=c["medium_activities"]: score+=2; reasons.append(f"{a} activities")
        if p>=c["medium_parameters"]: score+=1; reasons.append(f"{p} parameters")
        if v>=c["medium_variables"]: score+=1; reasons.append(f"{v} variables")
    elif kind=="activity":
        c=cfg["activity"]; t=norm(str(row.get("Activity Type",""))); d=int(row.get("Dependency Count",0)); depth=int(row.get("Nesting Depth",0))
        if any(norm(x) in t for x in c["complex_types"]): score+=4; reasons.append(f"type={row.get('Activity Type')}")
        elif any(norm(x) in t for x in c["medium_types"]): score+=2; reasons.append(f"type={row.get('Activity Type')}")
        if d>=c["complex_dependencies"]: score+=3; reasons.append(f"{d} dependencies")
        elif d: score+=1; reasons.append(f"{d} dependencies")
        if depth: score+=min(depth,2); reasons.append(f"nested depth {depth}")
        if row.get("Dynamic Expression Count",0): score+=1; reasons.append("dynamic expressions")
    elif kind=="dataflow":
        c=cfg["dataflow"]; t=int(row.get("Transformation Count",0)); lines=int(row.get("Script Lines",0))
        if t>=c["complex_transformations"] or lines>=c["complex_script_lines"]: score+=4; reasons.append(f"{t} transforms/{lines} lines")
        elif t>=c["medium_transformations"]: score+=2; reasons.append(f"{t} transformations")
    elif kind=="dataset":
        c=cfg["dataset"]; n=int(row.get("Schema Column Count",0))
        if n>=c["complex_schema_columns"]: score+=4; reasons.append(f"{n} schema columns")
        elif n>=c["medium_schema_columns"]: score+=2; reasons.append(f"{n} schema columns")
        if int(row.get("Parameter Count",0))>=2: score+=1; reasons.append("parameterized")
    level="COMPLEX" if score>=4 else "MEDIUM" if score>=2 else "SIMPLE"
    return level,"; ".join(reasons) or "no elevated complexity signals",score

def base_row(r: Resource) -> dict[str,Any]:
    p=r.props
    return {"Resource Name":r.name,"Resource Type":r.kind,"Source File":r.source_file,"ADF Type":safe(p.get("type")),"Description":safe(p.get("description")),"Folder":safe((p.get("folder") or {}).get("name") if isinstance(p.get("folder"),dict) else "","Root"),"Annotations":flat(p.get("annotations",[])),"Raw Properties JSON":flat(p,5000)}

def parse_resource(r: Resource, cfg: dict[str,Any]) -> list[dict[str,Any]]:
    p=r.props; b=base_row(r)
    if r.kind=="pipeline":
        acts=list(walk_activities(p.get("activities",[]))); types=Counter(str(a.get("type","")) for a,_,_,_ in acts)
        row={**b,"Activity Count":len(acts),"Top-level Activity Count":count(p.get("activities")),"Max Nesting Depth":max([d for _,_,d,_ in acts],default=0),"Activity Types":", ".join(f"{k}:{v}" for k,v in sorted(types.items())),"Parameter Count":count(p.get("parameters")),"Variable Count":count(p.get("variables")),"Concurrency":safe(p.get("concurrency"))}
        level,reason,score=classify("pipeline",row,cfg); row={"Complexity":level,"Complexity Score":score,"Complexity Reason":reason,**row}; return [row]
    if r.kind=="dataset":
        tp=p.get("typeProperties",{}) if isinstance(p.get("typeProperties",{}),dict) else {}; loc=tp.get("location",{}) if isinstance(tp.get("location",{}),dict) else {}
        row={**b,"Linked Service":ref_name(p.get("linkedServiceName")),"Schema Column Count":count(p.get("schema")),"Parameter Count":count(p.get("parameters")),"Location Type":safe(loc.get("type")),"Location JSON":flat(loc),"Type Properties JSON":flat(tp,2000)}
        level,reason,score=classify("dataset",row,cfg); return [{"Complexity":level,"Complexity Score":score,"Complexity Reason":reason,**row}]
    if r.kind=="dataflow":
        tp=p.get("typeProperties",{}) if isinstance(p.get("typeProperties",{}),dict) else {}; script=str(tp.get("script") or "")
        row={**b,"Source Count":count(tp.get("sources")),"Sink Count":count(tp.get("sinks")),"Transformation Count":count(tp.get("transformations")),"Script Lines":len(script.splitlines()),"Sources JSON":flat(tp.get("sources",[]),2000),"Sinks JSON":flat(tp.get("sinks",[]),2000)}
        level,reason,score=classify("dataflow",row,cfg); return [{"Complexity":level,"Complexity Score":score,"Complexity Reason":reason,**row}]
    if r.kind=="linked_service":
        tp=p.get("typeProperties",{}) if isinstance(p.get("typeProperties",{}),dict) else {}; red={k:("[REDACTED]" if any(x in k.lower() for x in ("password","secret","key","token","credential","connectionstring")) else v) for k,v in tp.items()}
        return [{**b,"Integration Runtime":ref_name(p.get("connectVia")) or "AutoResolve","Parameter Count":count(p.get("parameters")),"Connection Properties":flat(red,2000)}]
    if r.kind=="trigger":
        tp=p.get("typeProperties",{}) if isinstance(p.get("typeProperties",{}),dict) else {}; rec=tp.get("recurrence",{}) if isinstance(tp.get("recurrence",{}),dict) else {}; pls=p.get("pipelines",[]) if isinstance(p.get("pipelines",[]),list) else []
        return [{**b,"Runtime State":safe(p.get("runtimeState")),"Frequency":safe(rec.get("frequency") or tp.get("frequency")),"Interval":safe(rec.get("interval") or tp.get("interval")),"Time Zone":safe(rec.get("timeZone") or tp.get("timeZone")),"Pipeline Count":len(pls),"Pipelines":", ".join(ref_name(x.get("pipelineReference")) for x in pls if isinstance(x,dict))}]
    return [{**b,"Properties JSON":flat(p,5000)}]

def activity_rows(resources: list[Resource], cfg: dict[str,Any]) -> list[dict[str,Any]]:
    rows=[]
    for r in resources:
        if r.kind!="pipeline": continue
        for act,path,depth,parent in walk_activities(r.props.get("activities",[])):
            tp=act.get("typeProperties",{}) if isinstance(act.get("typeProperties",{}),dict) else {}; deps=act.get("dependsOn",[]) if isinstance(act.get("dependsOn",[]),list) else []
            text=json.dumps(act,ensure_ascii=False,default=str); expressions=len(re.findall(r"@\{|@\w+\(",text))
            row={"Pipeline Name":r.name,"Activity Name":safe(act.get("name")),"Activity Path":path,"Parent Activity":parent,"Nesting Depth":depth,"Activity Type":safe(act.get("type")),"Description":safe(act.get("description")),"Dependency Count":len(deps),"Depends On":", ".join(str(x.get("activity","")) for x in deps if isinstance(x,dict)),"Input Datasets":join_refs(act.get("inputs")),"Output Datasets":join_refs(act.get("outputs")),"Linked Service":ref_name(act.get("linkedServiceName") or act.get("linkedService")),"Dynamic Expression Count":expressions,"Policy Timeout":safe((act.get("policy") or {}).get("timeout") if isinstance(act.get("policy"),dict) else ""),"Policy Retries":safe((act.get("policy") or {}).get("retry") if isinstance(act.get("policy"),dict) else ""),"Type Properties JSON":flat(tp,3000),"Source File":r.source_file}
            level,reason,score=classify("activity",row,cfg); rows.append({"Complexity":level,"Complexity Score":score,"Complexity Reason":reason,**row})
    return rows

def write_csv(path: Path, rows: list[dict[str,Any]]) -> None:
    headers=[]
    for row in rows:
        for k in row:
            if k not in headers: headers.append(k)
    with open(path,"w",newline="",encoding="utf-8-sig") as f:
        w=csv.DictWriter(f,fieldnames=headers or ["Note"],extrasaction="ignore"); w.writeheader(); w.writerows(rows or [{"Note":"No records found"}])

def run(repo: Path, output: Path, config: dict[str,Any], max_depth=4) -> dict[str,Any]:
    roots=discover_roots(repo,max_depth); ctx=Context(repo.resolve(),output.resolve(),config); ingest(ctx,roots)
    grouped: dict[str,list[dict[str,Any]]]={}
    for r in ctx.resources:
        try: grouped.setdefault(r.kind,[]).extend(parse_resource(r,config))
        except Exception as e: ctx.issues.append(Issue("ERROR","PARSER_FAILURE",r.source_file,r.kind,str(e),False))
    grouped["activity"]=activity_rows(ctx.resources,config)
    known={"factory","pipeline","activity","dataset","dataflow","linked_service","integration_runtime","trigger","credential","managed_virtual_network","managed_private_endpoint"}
    for r in ctx.resources:
        if r.kind not in known: ctx.issues.append(Issue("WARNING","GENERIC_ONLY",r.source_file,r.kind,"Resource retained through generic parser",True))
    output.mkdir(parents=True,exist_ok=True)
    summary=[]
    for kind in sorted(grouped):
        rows=grouped[kind]; write_csv(output/f"{kind}s.csv",rows)
        c=Counter(str(x.get("Complexity","")) for x in rows)
        summary.append({"Resource Type":kind,"Count":len(rows),"Simple":c["SIMPLE"],"Medium":c["MEDIUM"],"Complex":c["COMPLEX"]})
    write_csv(output/"summary.csv",summary); write_csv(output/"validation_issues.csv",[x.row() for x in ctx.issues])
    manifest={"framework_version":VERSION,"generated_at":datetime.now(timezone.utc).isoformat(),"repository_root":str(ctx.repo_root),"detected_adf_roots":[str(x.relative_to(ctx.repo_root)) if x!=ctx.repo_root else "." for x in roots],"resource_file_count":len(ctx.resources),"summary":summary,"validation_issue_count":len(ctx.issues),"config":config}
    with open(output/"inventory.json","w",encoding="utf-8") as f: json.dump({"manifest":manifest,"resources":grouped,"validation_issues":[x.row() for x in ctx.issues]},f,indent=2,ensure_ascii=False,default=str)
    with open(output/"manifest.json","w",encoding="utf-8") as f: json.dump(manifest,f,indent=2,ensure_ascii=False)
    return manifest

def main() -> int:
    ap=argparse.ArgumentParser(description="Reusable ADF Git inventory framework")
    ap.add_argument("--repo",default=".",help="Repository or parent directory")
    ap.add_argument("--output",default="adf_inventory_output",help="Output directory")
    ap.add_argument("--config",help="Optional JSON rule overrides")
    ap.add_argument("--max-depth",type=int,default=4,help="ADF root discovery depth")
    ap.add_argument("--write-default-config",action="store_true")
    args=ap.parse_args()
    if args.write_default_config:
        with open("inventory_config.json","w",encoding="utf-8") as f: json.dump(DEFAULT_CONFIG,f,indent=2)
        print("Wrote inventory_config.json"); return 0
    repo=Path(args.repo).resolve(); output=Path(args.output); output=output if output.is_absolute() else repo/output
    manifest=run(repo,output,load_config(args.config),args.max_depth)
    print(f"ADF inventory complete: {manifest['resource_file_count']} resource files")
    print(f"ADF roots: {', '.join(manifest['detected_adf_roots'])}")
    print(f"Validation issues: {manifest['validation_issue_count']}")
    print(f"Output: {output}")
    return 0
if __name__=="__main__": raise SystemExit(main())
