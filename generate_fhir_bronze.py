#!/usr/bin/env python3
"""
Synthea-style synthetic FHIR R4 BRONZE dataset (seeded, reproducible), with
deliberately injected data-quality anomalies.

Run: python generate_fhir_bronze.py     (stdlib only)
"""
import json
import os
import random
from collections import Counter
from datetime import datetime, timedelta

SEED = 42
random.seed(SEED)

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fhir_bronze")
os.makedirs(OUT, exist_ok=True)

AS_OF = datetime(2026, 9, 28)
START = datetime(2025, 1, 1)
END = datetime(2026, 8, 31, 23, 59)

N_PAT, N_PRACT = 800, 60
manifest = []


def flag(rtype, rid, kind, detail):
    manifest.append(dict(resourceType=rtype, resource_id=rid, anomaly_type=kind, detail=detail))


def rand_dt(a, b):
    return a + timedelta(seconds=random.randint(0, int((b - a).total_seconds())))


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")


def uid(prefix, n):
    return f"{prefix}-{n:06d}"


FIRST = ["James", "Mary", "Robert", "Patricia", "Priya", "Arjun", "Wei", "Mei", "Carlos", "Sofia",
         "Omar", "Fatima", "Linda", "Michael", "Jennifer", "David", "Elizabeth", "Kenji", "Aiko", "Sarah"]
LAST = ["Smith", "Johnson", "Williams", "Garcia", "Patel", "Nguyen", "Kim", "Chen", "Singh", "Khan",
        "Ali", "Cohen", "Murphy", "Rossi", "Martinez", "Brown", "Davis", "Lopez", "Clark", "Lewis"]

ICD10 = ["I10", "E11.9", "J44.1", "I50.9", "N18.3", "J18.9", "K35.80", "S72.001A", "M54.5",
         "F32.9", "G43.909", "N39.0", "K21.9", "I63.9", "E78.5", "J45.909"]
SNOMED_COND = [("38341003", "Hypertension"), ("44054006", "Type 2 diabetes mellitus"),
               ("195967001", "Asthma"), ("84114007", "Heart failure"), ("13645005", "COPD")]
LOINC_OBS = [("8480-6", "Systolic blood pressure", "mm[Hg]"), ("8462-4", "Diastolic blood pressure", "mm[Hg]"),
             ("8867-4", "Heart rate", "/min"), ("2339-0", "Glucose", "mg/dL"),
             ("29463-7", "Body weight", "kg"), ("8310-5", "Body temperature", "Cel")]
CPT_PROC = [("93000", "Electrocardiogram"), ("80053", "Comprehensive metabolic panel"),
            ("71046", "Chest X-ray"), ("99213", "Office visit"), ("36415", "Blood draw")]
ENC_CLASS = [("AMB", "ambulatory"), ("EMER", "emergency"), ("IMP", "inpatient")]

# --------------------------------------------------------------------------- Organization (facilities)
ORGS_SRC = [
    ("St. Mary's Medical Center", "Chicago", "IL"), ("Riverside General Hospital", "Columbus", "OH"),
    ("Lakeshore Community Hospital", "Milwaukee", "WI"), ("Pinecrest Regional Medical Center", "Atlanta", "GA"),
    ("Sunbelt Health Center", "Dallas", "TX"), ("Bayview Specialty Hospital", "Miami", "FL"),
    ("Cascade Medical Center", "Seattle", "WA"), ("Golden Valley Hospital", "Sacramento", "CA"),
    ("Desert Springs Clinic", "Phoenix", "AZ"), ("Liberty Memorial Hospital", "Boston", "MA"),
]
orgs = []
for i, (name, city, state) in enumerate(ORGS_SRC, 1):
    orgs.append({
        "resourceType": "Organization", "id": uid("org", i),
        "identifier": [{"system": "http://hl7.org/fhir/sid/us-npi", "value": f"16{i:08d}"}],
        "name": name, "active": True,
        "address": [{"city": city, "state": state, "country": "US"}],
        "type": [{"coding": [{"system": "http://terminology.hl7.org/CodeSystem/organization-type",
                               "code": "prov", "display": "Healthcare Provider"}]}],
    })
org_ids = [o["id"] for o in orgs]

# duplicate facility: same hospital re-exported under a new id with a slightly different name
dup_org = json.loads(json.dumps(orgs[0]))
dup_org["id"] = uid("org", 11)
dup_org["name"] = dup_org["name"].upper()
orgs.append(dup_org)
flag("Organization", dup_org["id"], "duplicate_entity", f"same facility as {orgs[0]['id']} re-exported with a different id and shouting-case name")

# --------------------------------------------------------------------------- Practitioner
practitioners = []
for i in range(1, N_PRACT + 1):
    fn, ln = random.choice(FIRST), random.choice(LAST)
    practitioners.append({
        "resourceType": "Practitioner", "id": uid("pract", i),
        "identifier": [{"system": "http://hl7.org/fhir/sid/us-npi", "value": str(random.randint(1000000000, 1999999999))}],
        "name": [{"family": ln, "given": [fn]}], "active": True,
    })
for p in random.sample(practitioners, 5):
    p["identifier"][0]["value"] = p["identifier"][0]["value"][:9]
    flag("Practitioner", p["id"], "invalid_npi", "NPI has 9 digits instead of 10")
for p in random.sample(practitioners, 3):
    del p["name"]
    flag("Practitioner", p["id"], "missing_required_field", "name is absent")
pract_ids = [p["id"] for p in practitioners]

# --------------------------------------------------------------------------- Patient
GENDERS = ["male", "female"]
patients = []
for i in range(1, N_PAT + 1):
    fn, ln = random.choice(FIRST), random.choice(LAST)
    dob = rand_dt(datetime(1935, 1, 1), datetime(2023, 12, 31))
    patients.append({
        "resourceType": "Patient", "id": uid("pat", i),
        "identifier": [{"system": "http://hospital.example.org/mrn", "value": f"MRN{1000000 + i}"}],
        "name": [{"family": ln, "given": [fn]}],
        "gender": random.choice(GENDERS),
        "birthDate": dob.strftime("%Y-%m-%d"),
        "address": [{"postalCode": f"{random.randint(1000, 99950):05d}", "state": random.choice(["IL", "OH", "TX", "GA", "CA", "WA"])}],
        "active": True,
    })

pt_ids_pool = list(range(len(patients)))
random.shuffle(pt_ids_pool)


def take(n):
    chosen = pt_ids_pool[:n]
    del pt_ids_pool[:n]
    return chosen


for i in take(40):
    del patients[i]["birthDate"]
    flag("Patient", patients[i]["id"], "missing_required_field", "birthDate is absent")
for i in take(15):
    patients[i]["birthDate"] = (datetime(2027, 1, 1) + timedelta(days=random.randint(0, 700))).strftime("%Y-%m-%d")
    flag("Patient", patients[i]["id"], "future_birthdate", "birthDate is in the future")
for i in take(10):
    patients[i]["gender"] = random.choice(["Male", "FEMALE", "unk", "M", "F"])
    flag("Patient", patients[i]["id"], "invalid_code", "gender uses a value outside FHIR's administrative-gender ValueSet")
for i in take(6):
    patients[i]["name"] = [{"family": "TEST", "given": ["ZZTEST"]}]
    flag("Patient", patients[i]["id"], "test_record", "test patient that should be excluded")
for i in take(20):
    patients[i]["identifier"][0]["system"] = None
    flag("Patient", patients[i]["id"], "missing_code_system", "identifier.system is null, MRN can't be resolved to a namespace")

# duplicate patient: same MRN, new resource id, re-exported later (common bulk-export dedup problem)
dup_patients = []
for k, i in enumerate(take(40)):
    d = json.loads(json.dumps(patients[i]))
    d["id"] = uid("pat", N_PAT + 1 + k)
    dup_patients.append(d)
    flag("Patient", d["id"], "duplicate_resource", f"same identifier.MRN as {patients[i]['id']}, re-exported under a new logical id")
patients += dup_patients
all_patient_ids = [p["id"] for p in patients]

# --------------------------------------------------------------------------- Encounter
encounters = []
enc_n = 0
readmit_src = []
for p in patients:
    n_enc = max(0, round(random.paretovariate(2.2))) if random.random() < 0.9 else 0
    for _ in range(min(n_enc, 6)):
        enc_n += 1
        admit = rand_dt(START, END)
        cls_code, cls_disp = random.choice(ENC_CLASS)
        los_hours = {"AMB": random.randint(1, 3), "EMER": random.randint(2, 14), "IMP": random.randint(24, 240)}[cls_code]
        disc = admit + timedelta(hours=los_hours)
        org = random.choice(org_ids)
        pract = random.choice(pract_ids)
        e = {
            "resourceType": "Encounter", "id": uid("enc", enc_n),
            "status": "finished",
            "class": {"system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": cls_code, "display": cls_disp},
            "subject": {"reference": f"Patient/{p['id']}"},
            "participant": [{"individual": {"reference": f"Practitioner/{pract}"}}],
            "serviceProvider": {"reference": f"Organization/{org}"},
            "period": {"start": iso(admit), "end": iso(disc)},
            "reasonCode": [{"coding": [{"system": "http://hl7.org/fhir/sid/icd-10-cm", "code": random.choice(ICD10)}]}],
        }
        encounters.append(e)
        if cls_code == "IMP" and random.random() < 0.09 and disc + timedelta(days=3) <= END:
            readmit_src.append((p["id"], disc))

for pid, disc in readmit_src:
    enc_n += 1
    admit = disc + timedelta(days=random.randint(3, 28))
    if admit > END:
        continue
    cls_code, cls_disp = random.choice([("IMP", "inpatient"), ("EMER", "emergency")])
    disc2 = admit + timedelta(hours=random.randint(4, 200))
    encounters.append({
        "resourceType": "Encounter", "id": uid("enc", enc_n), "status": "finished",
        "class": {"system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": cls_code, "display": cls_disp},
        "subject": {"reference": f"Patient/{pid}"},
        "participant": [{"individual": {"reference": f"Practitioner/{random.choice(pract_ids)}"}}],
        "serviceProvider": {"reference": f"Organization/{random.choice(org_ids)}"},
        "period": {"start": iso(admit), "end": iso(disc2)},
        "reasonCode": [{"coding": [{"system": "http://hl7.org/fhir/sid/icd-10-cm", "code": random.choice(ICD10)}]}],
    })

enc_pool = list(range(len(encounters)))
random.shuffle(enc_pool)


def take_e(n):
    chosen = enc_pool[:n]
    del enc_pool[:n]
    return chosen


for i in take_e(35):
    encounters[i]["subject"]["reference"] = f"Patient/{uid('pat', 99000 + i)}"
    flag("Encounter", encounters[i]["id"], "dangling_reference", "subject references a Patient id that does not exist")
for i in take_e(15):
    del encounters[i]["serviceProvider"]
    flag("Encounter", encounters[i]["id"], "missing_required_field", "serviceProvider is absent")
for i in take_e(10):
    encounters[i]["serviceProvider"]["reference"] = f"Organization/{uid('org', 999)}"
    flag("Encounter", encounters[i]["id"], "dangling_reference", "serviceProvider references an Organization id that does not exist")
for i in take_e(20):
    encounters[i]["subject"]["reference"] = encounters[i]["subject"]["reference"].split("/")[-1]
    flag("Encounter", encounters[i]["id"], "malformed_reference", "reference is a bare id, missing the 'Patient/' resource-type prefix")
for i in take_e(30):
    st, dt_ = datetime.fromisoformat(encounters[i]["period"]["start"]), datetime.fromisoformat(encounters[i]["period"]["end"])
    encounters[i]["period"]["end"] = iso(st - timedelta(hours=random.randint(2, 48)))
    flag("Encounter", encounters[i]["id"], "period_end_before_start", "period.end is earlier than period.start")
for i in take_e(25):
    encounters[i]["status"] = random.choice(["Finished", "CLOSED", "done", "complete"])
    flag("Encounter", encounters[i]["id"], "invalid_status_code", "status uses a value outside FHIR's encounter-status ValueSet")
for i in take_e(20):
    st = datetime.fromisoformat(encounters[i]["period"]["start"])
    encounters[i]["period"]["start"] = st.strftime("%m/%d/%Y %H:%M")
    flag("Encounter", encounters[i]["id"], "invalid_date_format", "period.start is MM/DD/YYYY text instead of a FHIR instant")
for i in take_e(25):
    encounters[i]["class"] = {"code": encounters[i]["class"]["code"]}
    flag("Encounter", encounters[i]["id"], "missing_code_system", "class.system is absent, code can't be resolved to a CodeSystem")

# exact duplicate resources (identical bulk-export line re-sent)
dup_lines = [json.loads(json.dumps(encounters[i])) for i in random.sample(range(len(encounters)), 60)]
for d in dup_lines:
    flag("Encounter", d["id"], "duplicate_resource", "identical resource exported twice in the same bulk file")
encounters += dup_lines
random.shuffle(encounters)

enc_by_patient = {}
valid_enc_ids = set()
for e in encounters:
    valid_enc_ids.add(e["id"])
    ref = e["subject"]["reference"]
    pid = ref.split("/")[-1]
    enc_by_patient.setdefault(pid, []).append(e)

# --------------------------------------------------------------------------- Condition / Observation / Procedure
conditions, observations, procedures = [], [], []
cn = on = pn = 0
for e in encounters:
    pid = e["subject"]["reference"].split("/")[-1]
    enc_ref = f"Encounter/{e['id']}"
    if random.random() < 0.55:
        cn += 1
        use_snomed = random.random() < 0.4
        code, disp = random.choice(SNOMED_COND) if use_snomed else (random.choice(ICD10), None)
        system = "http://snomed.info/sct" if use_snomed else "http://hl7.org/fhir/sid/icd-10-cm"
        coding = {"system": system, "code": code}
        if disp:
            coding["display"] = disp
        conditions.append({
            "resourceType": "Condition", "id": uid("cond", cn),
            "clinicalStatus": {"coding": [{"code": "active"}]},
            "code": {"coding": [coding]},
            "subject": {"reference": f"Patient/{pid}"}, "encounter": {"reference": enc_ref},
            "onsetDateTime": e["period"]["start"],
        })
    for _ in range(random.randint(0, 3)):
        on += 1
        code, disp, unit = random.choice(LOINC_OBS)
        val = round(random.uniform(60, 180), 1)
        observations.append({
            "resourceType": "Observation", "id": uid("obs", on), "status": "final",
            "code": {"coding": [{"system": "http://loinc.org", "code": code, "display": disp}]},
            "subject": {"reference": f"Patient/{pid}"}, "encounter": {"reference": enc_ref},
            "effectiveDateTime": e["period"]["start"],
            "valueQuantity": {"value": val, "unit": unit},
        })
    if random.random() < 0.4:
        pn += 1
        code, disp = random.choice(CPT_PROC)
        procedures.append({
            "resourceType": "Procedure", "id": uid("proc", pn), "status": "completed",
            "code": {"coding": [{"system": "http://www.ama-assn.org/go/cpt", "code": code, "display": disp}]},
            "subject": {"reference": f"Patient/{pid}"}, "encounter": {"reference": enc_ref},
            "performedDateTime": e["period"]["start"],
        })

for c in random.sample(conditions, 40):
    c["code"] = {"coding": [{"code": random.choice(["XX999", "12345", "UNKNOWN"])}]}
    flag("Condition", c["id"], "invalid_code", "code is not a valid ICD-10-CM or SNOMED CT code, and system is missing")
for c in random.sample(conditions, 20):
    c["encounter"]["reference"] = f"Encounter/{uid('enc', 999999)}"
    flag("Condition", c["id"], "dangling_reference", "encounter references an Encounter id that does not exist")
for o in random.sample(observations, 25):
    o["valueQuantity"]["value"] = -abs(o["valueQuantity"]["value"])
    flag("Observation", o["id"], "implausible_value", "valueQuantity is negative for a vital sign that cannot be negative")
for o in random.sample(observations, 20):
    del o["subject"]
    flag("Observation", o["id"], "missing_required_field", "subject is absent")
for pr in random.sample(procedures, 15):
    pr["status"] = random.choice(["Done", "COMPLETE", "finished"])
    flag("Procedure", pr["id"], "invalid_status_code", "status uses a value outside FHIR's event-status ValueSet")

# --------------------------------------------------------------------------- Claim
claims = []
cln = 0
for e in encounters:
    if random.random() < 0.06:
        continue  # unbilled encounter, no claim at all
    cln += 1
    pid = e["subject"]["reference"].split("/")[-1]
    org = e.get("serviceProvider", {}).get("reference", f"Organization/{random.choice(org_ids)}")
    total = round(random.lognormvariate(8.0, 0.6) * (2 if e["class"].get("code") == "IMP" else 1), 2)
    claims.append({
        "resourceType": "Claim", "id": uid("claim", cln), "status": "active",
        "type": {"coding": [{"code": "institutional"}]},
        "patient": {"reference": f"Patient/{pid}"},
        "provider": {"reference": org},
        "item": [{"encounter": [{"reference": f"Encounter/{e['id']}"}]}],
        "billablePeriod": e["period"],
        "total": {"value": total, "currency": "USD"},
    })

for c in random.sample(claims, 35):
    c["patient"]["reference"] = f"Patient/{uid('pat', 98000 + random.randint(1, 900))}"
    flag("Claim", c["id"], "dangling_reference", "patient references a Patient id that does not exist")
for c in random.sample(claims, 30):
    c["item"][0]["encounter"][0]["reference"] = f"Encounter/{uid('enc', 999000 + random.randint(1, 900))}"
    flag("Claim", c["id"], "dangling_reference", "item.encounter references an Encounter id that does not exist")
for c in random.sample(claims, 25):
    c["total"]["value"] = -abs(c["total"]["value"])
    flag("Claim", c["id"], "implausible_value", "total.value is negative")
for c in random.sample(claims, 40):
    del c["total"]
    flag("Claim", c["id"], "missing_required_field", "total is absent")
for c in random.sample(claims, 20):
    c["status"] = random.choice(["Active", "CLOSED", "paid", "denied"])
    flag("Claim", c["id"], "invalid_status_code", "status uses a value outside FHIR's fm-status ValueSet")
for c in random.sample([x for x in claims if "total" in x], 15):
    c["total"]["currency"] = None
    flag("Claim", c["id"], "missing_code_system", "total.currency is null")

dup_claims = [json.loads(json.dumps(c)) for c in random.sample(claims, 45)]
for d in dup_claims:
    flag("Claim", d["id"], "duplicate_resource", "identical resource exported twice in the same bulk file")
claims += dup_claims
random.shuffle(claims)

# --------------------------------------------------------------------------- write NDJSON
def write_ndjson(name, rows):
    with open(os.path.join(OUT, f"{name}.ndjson"), "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


write_ndjson("Organization", orgs)
write_ndjson("Practitioner", practitioners)
write_ndjson("Patient", patients)
write_ndjson("Encounter", encounters)
write_ndjson("Condition", conditions)
write_ndjson("Observation", observations)
write_ndjson("Procedure", procedures)
write_ndjson("Claim", claims)

import csv
with open(os.path.join(OUT, "anomaly_manifest.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["resourceType", "resource_id", "anomaly_type", "detail"])
    w.writeheader()
    w.writerows(manifest)

print("counts:", {"Organization": len(orgs), "Practitioner": len(practitioners), "Patient": len(patients),
                  "Encounter": len(encounters), "Condition": len(conditions), "Observation": len(observations),
                  "Procedure": len(procedures), "Claim": len(claims)})
summary = Counter((m["resourceType"], m["anomaly_type"]) for m in manifest)
for (t, k), n in sorted(summary.items()):
    print(f"  {t:13s} {k:24s} {n}")
print("total anomalies flagged:", len(manifest))
