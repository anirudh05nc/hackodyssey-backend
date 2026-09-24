import os
import csv
import time
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import boto3
from botocore.exceptions import ClientError

app = FastAPI(
    docs_url=None,
    redoc_url=None,
    openapi_url=None
)

# Standardized origins for production, staging, and local environments
origins = [
    "https://hack26.anc-anirudh.online",
    "http://hack26.anc-anirudh.online",
    "https://hackodyssey.gfgkare.in",
    "http://hackodyssey.gfgkare.in",
    "https://hack26.gfgkare.in",
    "http://hack26.gfgkare.in",
    "http://localhost:5173",
]

# Enable Starlette CORS middleware with full wildcard origin regex support
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_origin_regex=r"^https?://.*",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# Custom response middleware to ensure CORS headers persist even across 500s/exceptions
@app.middleware("http")
async def cors_guarantee_middleware(request: Request, call_next):
    origin = request.headers.get("origin")
    if request.method == "OPTIONS":
        response = Response(status_code=200)
    else:
        try:
            response = await call_next(request)
        except Exception as exc:
            import traceback
            traceback.print_exc()
            response = JSONResponse(
                status_code=500,
                content={"detail": f"Internal Server Error: {str(exc)}"}
            )
    
    if origin:
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Credentials"] = "true"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS, PATCH, HEAD"
        response.headers["Access-Control-Allow-Headers"] = request.headers.get("access-control-request-headers", "*")
        response.headers["Access-Control-Expose-Headers"] = "*"
    return response

# Catch-all OPTIONS preflight route
@app.options("/{full_path:path}")
async def preflight_options_handler(full_path: str, request: Request):
    response = Response(status_code=200)
    origin = request.headers.get("origin")
    if origin:
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Credentials"] = "true"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS, PATCH, HEAD"
        response.headers["Access-Control-Allow-Headers"] = request.headers.get("access-control-request-headers", "*")
        response.headers["Access-Control-Expose-Headers"] = "*"
    return response

# Point boto3 to your local folder
current_dir = os.path.dirname(os.path.abspath(__file__))
os.environ['AWS_SHARED_CREDENTIALS_FILE'] = os.path.join(current_dir, 'credentials.ini')
os.environ['AWS_PROFILE'] = 'default'

# Now initialize your session
session = boto3.Session()

dynamodb = boto3.resource('dynamodb', region_name='ap-south-2') # Change to your region

TABLE_NAME = os.environ.get("TABLE_NAME", "euphoria26_teams")
table = dynamodb.Table(TABLE_NAME)

def ensure_table_exists():
    global table, TABLE_NAME
    try:
        table.load()
    except ClientError as e:
        err_code = e.response.get('Error', {}).get('Code')
        if err_code == 'ResourceNotFoundException':
            try:
                print(f"Creating DynamoDB table: {TABLE_NAME}...")
                table = dynamodb.create_table(
                    TableName=TABLE_NAME,
                    KeySchema=[{'AttributeName': 'TeamID', 'KeyType': 'HASH'}],
                    AttributeDefinitions=[{'AttributeName': 'TeamID', 'AttributeType': 'S'}],
                    BillingMode='PAY_PER_REQUEST'
                )
                table.wait_until_exists()
                print(f"Successfully created table {TABLE_NAME}")
            except Exception as create_err:
                print(f"Failed to auto-create table {TABLE_NAME}: {create_err}")

ensure_table_exists()

from botocore.config import Config
s3_client = boto3.client(
    's3',
    region_name='ap-south-2',
    endpoint_url='https://s3.ap-south-2.amazonaws.com',
    config=Config(signature_version='s3v4')
)
S3_BUCKET = os.environ.get("S3_BUCKET", "euphoria26-certificates")

def ensure_s3_bucket_exists():
    global s3_client, S3_BUCKET
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET)
    except ClientError as e:
        err_code = e.response.get('Error', {}).get('Code')
        if err_code in ('404', 'NoSuchBucket'):
            try:
                print(f"Creating S3 bucket: {S3_BUCKET} in ap-south-2...")
                s3_client.create_bucket(
                    Bucket=S3_BUCKET,
                    CreateBucketConfiguration={'LocationConstraint': 'ap-south-2'}
                )
            except Exception as create_err:
                print(f"Failed to auto-create S3 bucket {S3_BUCKET}: {create_err}")
    # Always ensure CORS is configured on the bucket
    try:
        s3_client.put_bucket_cors(
            Bucket=S3_BUCKET,
            CORSConfiguration={
                'CORSRules': [{
                    'AllowedHeaders': ['*'],
                    'AllowedMethods': ['PUT', 'GET', 'HEAD', 'POST', 'DELETE'],
                    'AllowedOrigins': ['*'],
                    'ExposeHeaders': ['ETag', 'x-amz-request-id', 'x-amz-id-2']
                }]
            }
        )
    except Exception as cors_err:
        print(f"Note: Could not set bucket CORS automatically: {cors_err}")

ensure_s3_bucket_exists()

# ─────────────────────────────────────────────────────────────────────────────
# AWS S3 Data Persistence & Backup Helpers
# ─────────────────────────────────────────────────────────────────────────────
def backup_teams_to_s3(teams_data: list):
    """Backs up all team records to AWS S3 at roster/teams.json for persistent cloud redundancy."""
    try:
        import json
        payload = json.dumps(teams_data, indent=2, default=str)
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key="roster/teams.json",
            Body=payload.encode('utf-8'),
            ContentType='application/json'
        )
    except Exception as e:
        print(f"S3 teams backup notice: {e}")

def backup_participants_to_s3(participants_data: list):
    """Backs up all participant records to AWS S3 at roster/participants.json for persistent cloud redundancy."""
    try:
        import json
        payload = json.dumps(participants_data, indent=2, default=str)
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key="roster/participants.json",
            Body=payload.encode('utf-8'),
            ContentType='application/json'
        )
    except Exception as e:
        print(f"S3 participants backup notice: {e}")

def backup_marks_to_s3(scores_data: dict):
    """Backs up jury marks and evaluations to AWS S3 at evaluations/jury_scores.json."""
    try:
        import json
        payload = json.dumps(scores_data, indent=2, default=str)
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key="evaluations/jury_scores.json",
            Body=payload.encode('utf-8'),
            ContentType='application/json'
        )
    except Exception as e:
        print(f"S3 marks backup notice: {e}")

class CertificateUploadRequest(BaseModel):
    team_id: str
    member_name: str
    file_name: str
    cert_type: Optional[str] = "Data Analytics Essentials"


class ProblemSelection(BaseModel):
    problem_title: str

class ToggleSelectionRequest(BaseModel):
    enabled: bool

class TimerLaunchRequest(BaseModel):
    duration: int

class DeployedLinkRequest(BaseModel):
    deployed_link: str

class TeamReviewRequest(BaseModel):
    status: str
    feedback: str
    score: int = 0

class ToggleDeleteProtectionRequest(BaseModel):
    enabled: bool

class ToggleFeedbackRequest(BaseModel):
    enabled: bool

class ProblemCsvDirectUploadRequest(BaseModel):
    csv_content: str

class UpdatePhaseRequest(BaseModel):
    phase_index: int

class AnnouncementRequest(BaseModel):
    text: str

class FeedbackSubmissionRequest(BaseModel):
    team_id: str
    reg_no: str
    how_was_event: str
    improvements: str
    discomfort: str
    other: str
    rating: int  # 1-5

class TeamImportItem(BaseModel):
    TeamID: str
    TeamName: str
    Password: str
    LeaderName: str
    LeaderEmail: str
    LeaderPhone: str
    LeaderRegNo: str
    TransactionID: str = ""
    Status: str = "SUCCESS"
    SubmittedAt: str = ""

class ParticipantImportItem(BaseModel):
    TeamId: str
    Name: str
    RegNo: str
    Email: str
    Phone: str
    Gender: str
    Branch: str
    Year: int
    Accommodation: str = ""
    HostelName: str = ""
    RoomNo: str = ""
    WardenName: str = ""
    WardenPhone: str = ""

class ImportRequest(BaseModel):
    teams: List[TeamImportItem]
    participants: List[ParticipantImportItem]

class DeleteAllRequest(BaseModel):
    password: str

# ─────────────────────────────────────────────────────────────────────────────
# New models: Jury Portal, Problem Statements, Leaderboard
# ─────────────────────────────────────────────────────────────────────────────

VALID_SDG_IDS = {'SDG2', 'SDG3', 'SDG4', 'SDG6', 'SDG11', 'SDG13', 'HARDWARE'}

SDG_CATEGORY_LABELS = {
    'SDG2':     'Zero Hunger & Sustainable Agriculture',
    'SDG3':     'Good Health & Well-Being Innovation',
    'SDG4':     'Quality Education & Lifelong Learning',
    'SDG6':     'Clean Water & Sanitation',
    'SDG11':    'Sustainable Cities & Communities',
    'SDG13':    'Climate Action & Environmental Monitoring',
    'HARDWARE': 'Hardware',
}

JURY_CREDENTIALS = {
    'jury1': 'hack26jury',
    'jury2': 'hack26jury',
    'jury3': 'hack26jury',
}

class ProblemStatementItem(BaseModel):
    problem_id: str
    sdg_id: str
    title: str
    description: str = ""
    requirements: str = ""
    expectations: str = ""

class AssignProblemRequest(BaseModel):
    problem_id: Optional[str] = None   # None = unassign
    problem_title: Optional[str] = None
    sdg_id: Optional[str] = None

class JuryLoginRequest(BaseModel):
    username: str
    password: str

class JuryScoreSubmit(BaseModel):
    juror_id: str
    team_id: str
    review_round: Optional[str] = "review1"
    innovation: int     # 0-25
    execution: int      # 0-25
    impact: int         # 0-25
    presentation: int   # 0-25

class ToggleLeaderboardRequest(BaseModel):
    visible: bool

class SetThresholdRequest(BaseModel):
    threshold: int
    published: bool = True
    visible: bool = True

class ToggleThresholdRequest(BaseModel):
    visible: bool


@app.api_route("/", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "PATCH"])
def read_root():
    return HTMLResponse(content="""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Euphoria 26 API</title>
        <meta charset="utf-8">  
        <style>
            body { font-family: sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; background-color: #f0f2f5; margin: 0; }
            .container { text-align: center; padding: 2rem; background: white; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
            h1 { color: #1a73e8; margin-bottom: 0.5rem; }
            p { color: #5f6368; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Euphoria 26</h1>
            <p>API Server is active and running successfully.</p>
        </div>
    </body>
    </html>
    """, status_code=200)


@app.get("/teams/all")
def get_all_items():
    try:
        response = table.scan()
        data = response.get('Items', [])
        filtered_data = [t for t in data if t.get('TeamID') != "SYSTEM_SETTINGS"]
        return {"count": len(filtered_data), "items": filtered_data}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.get("/teams/{partition_id}")
def get_single_item(partition_id: str):
    if partition_id in ("SYSTEM_SETTINGS", "purge-all-data", "all"):
        raise HTTPException(status_code=404, detail="Team not found")
    try:
        response = table.get_item(
            Key={
                'TeamID': partition_id
            }
        )
        
        item = response.get('Item')
        if not item:
            raise HTTPException(status_code=404, detail="Item not found")
            
        return item
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


# ─────────────────────────────────────────────────────────────────────────────
# Participant Endpoints (Dynamically parsed from AWS DynamoDB & S3)
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/participants/all")
def get_all_participants():
    """
    Parses and returns all participants across all teams directly from AWS DynamoDB.
    Flattens members and enriches each record with team metadata.
    """
    try:
        response = table.scan()
        items = response.get('Items', [])
        all_participants = []
        for team in items:
            t_id = team.get('TeamID')
            if t_id == "SYSTEM_SETTINGS":
                continue
            t_name = team.get('Team Name') or team.get('TeamName') or t_id
            selected_prob = team.get('SelectedProblem') or team.get('AdminAssignedProblemTitle') or ""
            status = team.get('Status') or "SUCCESS"
            score = team.get('EvaluationScore') or 0
            certs_map = team.get('Certificates', {})

            members = team.get('Members', [])
            for m in members:
                m_copy = dict(m)
                m_copy['TeamID'] = t_id
                m_copy['TeamName'] = t_name
                m_copy['SelectedProblem'] = selected_prob
                m_copy['TeamStatus'] = status
                m_copy['EvaluationScore'] = score
                m_name = m_copy.get('name', '')
                m_copy['Certificates'] = certs_map.get(m_name, []) if isinstance(certs_map, dict) else []
                all_participants.append(m_copy)

        return {
            "count": len(all_participants),
            "participants": all_participants
        }
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.get("/participants/stats")
def get_participants_stats():
    """
    Calculates aggregate metrics (gender, branch, accommodation) live from AWS DynamoDB.
    """
    try:
        response = table.scan()
        items = response.get('Items', [])
        total_teams = 0
        total_participants = 0
        gender_counts = {}
        branch_counts = {}
        year_counts = {}
        accommodation_counts = {"Hosteller": 0, "DayScholar": 0, "Unspecified": 0}
        feedback_count = 0

        for team in items:
            if team.get('TeamID') == "SYSTEM_SETTINGS":
                continue
            total_teams += 1
            members = team.get('Members', [])
            for m in members:
                total_participants += 1
                g = (m.get('gender') or 'Unspecified').strip().capitalize()
                gender_counts[g] = gender_counts.get(g, 0) + 1
                b = (m.get('branch') or 'Unspecified').strip().upper()
                branch_counts[b] = branch_counts.get(b, 0) + 1
                y = str(m.get('year') or 'Unspecified').strip()
                year_counts[y] = year_counts.get(y, 0) + 1
                acc = (m.get('accommodation') or '').strip().lower()
                if 'yes' in acc or 'hostel' in acc:
                    accommodation_counts["Hosteller"] += 1
                elif 'no' in acc or 'day' in acc:
                    accommodation_counts["DayScholar"] += 1
                else:
                    accommodation_counts["Unspecified"] += 1
                if m.get('FeedbackSubmitted'):
                    feedback_count += 1

        return {
            "total_teams": total_teams,
            "total_participants": total_participants,
            "gender_distribution": gender_counts,
            "branch_distribution": branch_counts,
            "year_distribution": year_counts,
            "accommodation_metrics": accommodation_counts,
            "feedback_submitted_count": feedback_count
        }
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.get("/participants/export-csv")
def export_participants_csv():
    """
    Exports all participants stored in DynamoDB as a streaming CSV response.
    """
    import io
    try:
        data = get_all_participants()
        participants = data.get("participants", [])
        
        output = io.StringIO()
        fieldnames = [
            "TeamID", "TeamName", "RegNo", "Name", "Email", "Phone",
            "Gender", "Branch", "Year", "Accommodation", "HostelName",
            "RoomNo", "SelectedProblem", "EvaluationScore", "FeedbackSubmitted"
        ]
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        for p in participants:
            writer.writerow({
                "TeamID": p.get("TeamID", ""),
                "TeamName": p.get("TeamName", ""),
                "RegNo": p.get("regNo", ""),
                "Name": p.get("name", ""),
                "Email": p.get("email", ""),
                "Phone": p.get("phone", ""),
                "Gender": p.get("gender", ""),
                "Branch": p.get("branch", ""),
                "Year": p.get("year", ""),
                "Accommodation": p.get("accommodation", ""),
                "HostelName": p.get("hostelName", ""),
                "RoomNo": p.get("roomNo", ""),
                "SelectedProblem": p.get("SelectedProblem", ""),
                "EvaluationScore": p.get("EvaluationScore", 0),
                "FeedbackSubmitted": p.get("FeedbackSubmitted", False),
            })
        
        csv_string = output.getvalue()
        return Response(
            content=csv_string,
            media_type="text/csv",
            headers={
                "Content-Disposition": "attachment; filename=euphoria26_participants.csv",
                "Access-Control-Expose-Headers": "Content-Disposition"
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/participants/{reg_no}")
def get_participant_by_reg_no(reg_no: str):
    """
    Searches for a participant by registration number directly in AWS DynamoDB.
    """
    clean_reg = reg_no.strip().lower()
    try:
        response = table.scan()
        items = response.get('Items', [])
        for team in items:
            if team.get('TeamID') == "SYSTEM_SETTINGS":
                continue
            members = team.get('Members', [])
            for m in members:
                if str(m.get('regNo') or '').strip().lower() == clean_reg:
                    m_copy = dict(m)
                    m_copy['TeamID'] = team.get('TeamID')
                    m_copy['TeamName'] = team.get('Team Name') or team.get('TeamName')
                    m_copy['SelectedProblem'] = team.get('SelectedProblem') or ""
                    m_copy['EvaluationScore'] = team.get('EvaluationScore') or 0
                    return m_copy
        raise HTTPException(status_code=404, detail=f"Participant with RegNo '{reg_no}' not found in DynamoDB.")
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])



@app.post("/teams/{team_id}/select-problem")
def select_problem(team_id: str, selection: ProblemSelection):
    if team_id == "SYSTEM_SETTINGS":
        raise HTTPException(status_code=400, detail="Invalid team selection request")
    try:
        response = table.get_item(Key={'TeamID': team_id})
        team = response.get('Item')
        if not team:
            raise HTTPException(status_code=404, detail="Team not found")
        
        current_selection = team.get('SelectedProblem')
        if current_selection:
            if current_selection == selection.problem_title:
                return {"message": "Problem already selected and locked.", "selected_problem": current_selection}
            raise HTTPException(status_code=400, detail="Challenge selection is locked and cannot be changed.")
        
        scan_resp = table.scan()
        all_teams = scan_resp.get('Items', [])
        count = sum(1 for t in all_teams if t.get('TeamID') != "SYSTEM_SETTINGS" and t.get('SelectedProblem') == selection.problem_title)
        
        if count >= 3:
            raise HTTPException(
                status_code=400, 
                detail=f"Challenge '{selection.problem_title}' is full. Maximum 3 teams allowed."
            )
        
        table.update_item(
            Key={'TeamID': team_id},
            UpdateExpression="set SelectedProblem = :val",
            ExpressionAttributeValues={':val': selection.problem_title}
        )
        
        return {"message": "Problem selection locked successfully.", "selected_problem": selection.problem_title}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.get("/problems/selection-counts")
def get_problem_selection_counts():
    try:
        response = table.scan()
        items = response.get('Items', [])
        counts = {}
        for item in items:
            if item.get('TeamID') == "SYSTEM_SETTINGS":
                continue
            prob = item.get('SelectedProblem')
            if prob:
                counts[prob] = counts.get(prob, 0) + 1
        return counts
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.post("/problems/revoke-all")
def revoke_all_selections():
    try:
        settings_res = table.get_item(Key={'TeamID': 'SYSTEM_SETTINGS'})
        settings_item = settings_res.get('Item', {})
        if settings_item.get('DeleteProtectionActive', False):
            raise HTTPException(
                status_code=400, 
                detail="Action Denied: Delete Protection is currently active and preventing selection modifications."
            )
            
        response = table.scan()
        items = response.get('Items', [])
        
        for item in items:
            team_id = item.get('TeamID')
            if team_id == "SYSTEM_SETTINGS":
                continue
            if 'SelectedProblem' in item:
                table.update_item(
                    Key={'TeamID': team_id},
                    UpdateExpression="remove SelectedProblem"
                )
        return {"message": "All problem statements revoked successfully."}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.get("/settings")
def get_settings():
    import time as _time
    try:
        response = table.get_item(Key={'TeamID': 'SYSTEM_SETTINGS'})
        item = response.get('Item')
        if not item:
            initial_settings = {
                'TeamID': 'SYSTEM_SETTINGS',
                'SelectionEnabled': False,
                'TimerLaunched': False,
                'TimerStartTime': 0,
                'TimerDuration': 0,
                'DeleteProtectionActive': False,
                'ProblemsCsvUploaded': False,
                'FeedbackEnabled': False,
                'LeaderboardVisible': False,
                'ThresholdValue': 0,
                'ThresholdPublished': False,
                'ThresholdVisible': False,
                'CurrentPhaseIndex': 0,
                'Announcements': []
            }
            table.put_item(Item=initial_settings)
            return {
                'SelectionEnabled': False,
                'TimerLaunched': False,
                'TimerStartTime': 0,
                'TimerDuration': 0,
                'DeleteProtectionActive': False,
                'ProblemsCsvUploaded': False,
                'FeedbackEnabled': False,
                'LeaderboardVisible': False,
                'ThresholdValue': 0,
                'ThresholdPublished': False,
                'ThresholdVisible': False,
                'CurrentPhaseIndex': 0,
                'Announcements': [],
                'ServerTime': int(_time.time())
            }
        return {
            'SelectionEnabled': item.get('SelectionEnabled', False),
            'TimerLaunched': item.get('TimerLaunched', False),
            'TimerStartTime': int(item.get('TimerStartTime', 0)),
            'TimerDuration': int(item.get('TimerDuration', 0)),
            'DeleteProtectionActive': item.get('DeleteProtectionActive', False),
            'ProblemsCsvUploaded': item.get('ProblemsCsvUploaded', False),
            'FeedbackEnabled': item.get('FeedbackEnabled', False),
            'LeaderboardVisible': bool(item.get('LeaderboardVisible', False)),
            'ThresholdValue': int(item.get('ThresholdValue', 0)),
            'ThresholdPublished': bool(item.get('ThresholdPublished', False)),
            'ThresholdVisible': bool(item.get('ThresholdVisible', False)),
            'CurrentPhaseIndex': int(item.get('CurrentPhaseIndex', 0)),
            'Announcements': item.get('Announcements', []),
            'ServerTime': int(_time.time())
        }
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.get("/announcements")
def get_announcements():
    try:
        response = table.get_item(Key={'TeamID': 'SYSTEM_SETTINGS'})
        item = response.get('Item', {})
        announcements = item.get('Announcements', [])
        return {"announcements": announcements}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.post("/announcements")
def publish_announcement(req: AnnouncementRequest):
    if not req.text.strip():
        raise HTTPException(status_code=400, detail="Announcement text cannot be empty.")
    try:
        response = table.get_item(Key={'TeamID': 'SYSTEM_SETTINGS'})
        item = response.get('Item', {}) if response else {}
        announcements = item.get('Announcements', []) if item else []
        new_ann = {
            "id": int(time.time() * 1000),
            "timestamp": time.strftime("%I:%M:%S %p"),
            "text": req.text.strip()
        }
        updated = [new_ann] + list(announcements)
        table.update_item(
            Key={'TeamID': 'SYSTEM_SETTINGS'},
            UpdateExpression="set Announcements = :val",
            ExpressionAttributeValues={':val': updated}
        )
        return {"message": "Announcement published successfully.", "announcement": new_ann, "announcements": updated}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.delete("/announcements/{announcement_id}")
def delete_announcement(announcement_id: int):
    try:
        response = table.get_item(Key={'TeamID': 'SYSTEM_SETTINGS'})
        item = response.get('Item', {}) if response else {}
        announcements = item.get('Announcements', []) if item else []
        updated = [a for a in announcements if int(a.get('id', 0)) != int(announcement_id)]
        table.update_item(
            Key={'TeamID': 'SYSTEM_SETTINGS'},
            UpdateExpression="set Announcements = :val",
            ExpressionAttributeValues={':val': updated}
        )
        return {"message": "Announcement deleted successfully.", "announcements": updated}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])



@app.post("/settings/update-phase")
def update_phase(req: UpdatePhaseRequest):
    try:
        if req.phase_index < 0 or req.phase_index > 8:
            raise HTTPException(status_code=400, detail="Invalid phase index. Must be between 0 and 8.")
        table.update_item(
            Key={'TeamID': 'SYSTEM_SETTINGS'},
            UpdateExpression="set CurrentPhaseIndex = :val",
            ExpressionAttributeValues={':val': req.phase_index}
        )
        return {"message": "Roadmap phase index updated successfully.", "CurrentPhaseIndex": req.phase_index}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.post("/settings/toggle-selection")
def toggle_selection(req: ToggleSelectionRequest):
    try:
        # Gate: selection can only be enabled if problems CSV has been uploaded
        if req.enabled:
            settings_res = table.get_item(Key={'TeamID': 'SYSTEM_SETTINGS'})
            settings_item = settings_res.get('Item', {})
            if not settings_item.get('ProblemsCsvUploaded', False):
                raise HTTPException(
                    status_code=400,
                    detail="Action Denied: Problem Statements CSV has not been uploaded to S3 yet. Upload it first before enabling selection."
                )
        table.update_item(
            Key={'TeamID': 'SYSTEM_SETTINGS'},
            UpdateExpression="set SelectionEnabled = :val",
            ExpressionAttributeValues={':val': req.enabled}
        )
        return {"message": "Selection configuration updated successfully.", "enabled": req.enabled}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.post("/settings/launch-timer")
def launch_timer(req: TimerLaunchRequest):
    import time
    expiry_time = int(time.time()) + req.duration
    try:
        table.update_item(
            Key={'TeamID': 'SYSTEM_SETTINGS'},
            UpdateExpression="set TimerLaunched = :l, TimerStartTime = :s, TimerDuration = :d, SelectionEnabled = :sel",
            ExpressionAttributeValues={
                ':l': True,
                ':s': expiry_time,
                ':d': req.duration,
                ':sel': True
            }
        )
        return {"message": "Timer launched successfully.", "TimerStartTime": expiry_time, "TimerDuration": req.duration, "ServerTime": int(time.time())}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.post("/settings/reset-timer")
def reset_timer():
    try:
        table.update_item(
            Key={'TeamID': 'SYSTEM_SETTINGS'},
            UpdateExpression="set TimerLaunched = :l, TimerStartTime = :s, TimerDuration = :d, SelectionEnabled = :sel",
            ExpressionAttributeValues={
                ':l': False,
                ':s': 0,
                ':d': 0,
                ':sel': False
            }
        )
        return {"message": "Timer reset successfully."}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.post("/api/certificates/generate-upload-url")
def generate_upload_url(req: CertificateUploadRequest):
    try:
        # Validate certificate type
        cert_type = req.cert_type or "Data Analytics Essentials"
        allowed_types = ["Data Analytics Essentials", "mongoDB", "Cloud"]
        if cert_type not in allowed_types:
            raise HTTPException(status_code=400, detail=f"Invalid certificate type. Must be 'Data Analytics Essentials', 'mongoDB', or 'Cloud'.")

        # Get team item to extract registration ID and current certificates count
        res = table.get_item(Key={'TeamID': req.team_id})
        team = res.get('Item')
        if not team:
            raise HTTPException(status_code=404, detail="Team not found")
        
        # Find member registration number from embedded list of members
        members = team.get('Members', [])
        reg_no = None
        for m in members:
            if m.get('name', '').lower().strip() == req.member_name.lower().strip():
                reg_no = m.get('regNo') or m.get('reg_no')
                break
        
        # Fallback to Leader RegNo if name matches Leader Name
        if not reg_no:
            if team.get('Leader Name', '').lower().strip() == req.member_name.lower().strip():
                reg_no = team.get('Leader RegNo')
                
        # Final fallback if registration number not found
        if not reg_no:
            reg_no = req.member_name.replace(" ", "_")
            
        # Count existing certificates for this member
        certs_map = team.get('Certificates', {})
        if not certs_map:
            certs_map = {}
        existing_certs = certs_map.get(req.member_name, [])
        if not isinstance(existing_certs, list):
            existing_certs = []
            
        # Construct the key according to the schema: certificates/team_{TeamId}/{RegNo}_{CertType}.pdf
        s3_key = f"certificates/team_{req.team_id}/{reg_no}_{cert_type}.pdf"
        
        # Limit checking and updating DB references
        if s3_key not in existing_certs:
            if len(existing_certs) >= 2:
                raise HTTPException(status_code=400, detail="Upload limit reached. Maximum 2 certificates allowed per participant.")
            
            # 1. Initialize Certificates Map if it does not exist
            table.update_item(
                Key={'TeamID': req.team_id},
                UpdateExpression="SET Certificates = if_not_exists(Certificates, :empty_map)",
                ExpressionAttributeValues={":empty_map": {}}
            )
            
            # 2. Append the new S3 key path string to Certificates[member_name]
            table.update_item(
                Key={'TeamID': req.team_id},
                UpdateExpression="SET Certificates.#member = list_append(if_not_exists(Certificates.#member, :empty_list), :new_key)",
                ExpressionAttributeNames={"#member": req.member_name},
                ExpressionAttributeValues={
                    ":new_key": [s3_key],
                    ":empty_list": []
                }
            )
        
        presigned_url = s3_client.generate_presigned_url(
            ClientMethod='put_object',
            Params={
                'Bucket': S3_BUCKET,
                'Key': s3_key,
                'ContentType': 'application/pdf'
            },
            ExpiresIn=3600
        )
        
        return {
            "presigned_url": presigned_url,
            "s3_key": s3_key
        }
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


class CertificateDeleteRequest(BaseModel):
    team_id: str
    member_name: str
    s3_key: str
    password: str

@app.post("/api/admin/certificates/delete")
def delete_certificate(req: CertificateDeleteRequest):
    if req.password != "delete":
        raise HTTPException(status_code=403, detail="Unauthorized: Incorrect deletion authorization key.")
        
    try:
        # 1. Delete object from S3
        s3_client.delete_object(
            Bucket=S3_BUCKET,
            Key=req.s3_key
        )
        
        # 2. Get the team record to update the database mapping
        res = table.get_item(Key={'TeamID': req.team_id})
        team = res.get('Item')
        if not team:
            raise HTTPException(status_code=404, detail="Team not found")
            
        certs_map = team.get('Certificates', {})
        member_certs = certs_map.get(req.member_name, [])
        if req.s3_key in member_certs:
            member_certs.remove(req.s3_key)
            certs_map[req.member_name] = member_certs
            
            # Update the team record
            table.update_item(
                Key={'TeamID': req.team_id},
                UpdateExpression="SET Certificates = :certs",
                ExpressionAttributeValues={":certs": certs_map}
            )
            
        return {"message": "Certificate successfully deleted from S3 and database."}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.get("/api/admin/certificates/presign-get")
def presign_get_certificate(s3_key: str):
    try:
        url = s3_client.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': S3_BUCKET,
                'Key': s3_key
            },
            ExpiresIn=3600
        )
        return {"url": url}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=str(e))


class PurgeAllCertsRequest(BaseModel):
    password: str

@app.post("/api/admin/certificates/delete-all")
def purge_all_certificates(req: PurgeAllCertsRequest):
    if req.password != "delete":
        raise HTTPException(status_code=403, detail="Unauthorized: Incorrect deletion authorization key.")
    
    try:
        # 1. Scan DynamoDB to find all team records
        response = table.scan()
        items = response.get('Items', [])
        
        all_s3_keys = []
        for item in items:
            certs_map = item.get('Certificates', {})
            if isinstance(certs_map, dict):
                for member, keys in certs_map.items():
                    if isinstance(keys, list):
                        all_s3_keys.extend(keys)
        
        # 2. Bulk delete keys from S3 (boto3 delete_objects)
        if all_s3_keys:
            for i in range(0, len(all_s3_keys), 1000):
                chunk = all_s3_keys[i:i+1000]
                delete_objects = {'Objects': [{'Key': k} for k in chunk]}
                s3_client.delete_objects(
                    Bucket=S3_BUCKET,
                    Delete=delete_objects
                )
        
        # 3. Update DynamoDB items to remove Certificates mapping
        for item in items:
            team_id = item.get('TeamID')
            if team_id == 'SYSTEM_SETTINGS':
                continue
            table.update_item(
                Key={'TeamID': team_id},
                UpdateExpression="REMOVE Certificates"
            )
            
        return {"message": f"Successfully purged {len(all_s3_keys)} certificate files from S3 and database."}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])



# Target S3 buckets helper
def get_target_s3_buckets() -> list:
    """
    Returns list of target S3 buckets to synchronize files with.
    Ensures euphoria26-certificates is always updated alongside active S3_BUCKET and hackodyssey-certificates.
    """
    buckets = [S3_BUCKET, "euphoria26-certificates", "hackodyssey-certificates"]
    unique = []
    for b in buckets:
        if b and b not in unique:
            unique.append(b)
    return unique


# S3 key for the problem statements CSV
PROBLEMS_CSV_S3_KEY = "problemstatements/problems.csv"


@app.post("/api/problems/upload-direct")
async def upload_problems_csv_direct(request: Request):
    """
    Direct server-side upload of Problem Statements CSV to S3.
    Bypasses browser-to-S3 CORS and signature restrictions.
    Synchronizes across euphoria26-certificates and target buckets.
    Accepts JSON {"csv_content": "..."}, multipart FormData with file, or raw text.
    """
    csv_content = ""
    content_type = request.headers.get("content-type", "")

    if "application/json" in content_type:
        try:
            body = await request.json()
            csv_content = body.get("csv_content", "")
        except Exception:
            pass
    elif "multipart/form-data" in content_type:
        try:
            form = await request.form()
            file_item = form.get("file")
            if file_item and hasattr(file_item, "read"):
                csv_bytes = await file_item.read()
                csv_content = csv_bytes.decode('utf-8', errors='replace')
            elif file_item and isinstance(file_item, str):
                csv_content = file_item
            else:
                csv_content = str(form.get("csv_content", ""))
        except Exception:
            pass

    if not csv_content:
        # Fallback to reading raw body
        try:
            body_bytes = await request.body()
            if body_bytes:
                text = body_bytes.decode('utf-8', errors='replace')
                if text.strip().startswith("{") and "csv_content" in text:
                    import json
                    parsed = json.loads(text)
                    csv_content = parsed.get("csv_content", "")
                else:
                    csv_content = text
        except Exception:
            pass

    if not csv_content or not csv_content.strip():
        raise HTTPException(status_code=400, detail="CSV content cannot be empty.")

    # Validate and parse problem statement rows
    parsed_items = _parse_problems_csv(csv_content)
    if not parsed_items:
        raise HTTPException(status_code=400, detail="Uploaded CSV contains no valid problem statement records. Please verify headers.")

    saved_buckets = []
    target_buckets = get_target_s3_buckets()
    for b in target_buckets:
        try:
            s3_client.put_object(
                Bucket=b,
                Key=PROBLEMS_CSV_S3_KEY,
                Body=csv_content.encode('utf-8'),
                ContentType='text/csv'
            )
            saved_buckets.append(b)
        except ClientError as s3_err:
            err_code = s3_err.response.get('Error', {}).get('Code')
            if err_code in ('NoSuchBucket', '404'):
                try:
                    ensure_s3_bucket_exists()
                    s3_client.put_object(
                        Bucket=b,
                        Key=PROBLEMS_CSV_S3_KEY,
                        Body=csv_content.encode('utf-8'),
                        ContentType='text/csv'
                    )
                    saved_buckets.append(b)
                except Exception as inner_e:
                    print(f"Notice: S3 write retry to bucket {b} failed: {inner_e}")
            else:
                print(f"Notice: S3 write to bucket {b} returned: {s3_err}")
        except Exception as e:
            print(f"Notice: S3 write to bucket {b} failed: {e}")

    if not saved_buckets:
        raise HTTPException(status_code=500, detail="Failed to store problem statements CSV in S3 buckets.")

    # Update DynamoDB SYSTEM_SETTINGS
    try:
        table.update_item(
            Key={'TeamID': 'SYSTEM_SETTINGS'},
            UpdateExpression="set ProblemsCsvUploaded = :val",
            ExpressionAttributeValues={':val': True}
        )
    except Exception as e:
        print(f"Notice: Updating SYSTEM_SETTINGS for problems upload failed: {e}")

    invalidate_problems_cache()
    return {
        "message": f"Problem statements CSV ({len(parsed_items)} items) successfully saved to S3.",
        "s3_key": PROBLEMS_CSV_S3_KEY,
        "buckets": saved_buckets,
        "count": len(parsed_items)
    }


@app.post("/api/problems/upload-csv")
def get_problems_csv_upload_url():
    """
    Generates a presigned S3 PUT URL for the admin to upload the problem
    statements CSV to S3 at problemstatements/problems.csv.
    After the URL is generated, marks ProblemsCsvUploaded = True in DynamoDB.
    """
    try:
        presigned_url = s3_client.generate_presigned_url(
            ClientMethod='put_object',
            Params={
                'Bucket': S3_BUCKET,
                'Key': PROBLEMS_CSV_S3_KEY,
                'ContentType': 'text/csv'
            },
            ExpiresIn=3600
        )
        table.update_item(
            Key={'TeamID': 'SYSTEM_SETTINGS'},
            UpdateExpression="set ProblemsCsvUploaded = :val",
            ExpressionAttributeValues={':val': True}
        )
        return {
            "presigned_url": presigned_url,
            "s3_key": PROBLEMS_CSV_S3_KEY,
            "message": "Presigned upload URL generated. Upload the CSV via PUT request to this URL."
        }
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.get("/api/problems/raw-csv")
def get_problems_raw_csv():
    """
    Directly streams or returns the Problem Statements CSV text from S3 via backend,
    checking all target buckets.
    """
    for b in get_target_s3_buckets():
        try:
            response = s3_client.get_object(
                Bucket=b,
                Key=PROBLEMS_CSV_S3_KEY
            )
            content = response['Body'].read().decode('utf-8')
            if content and content.strip():
                return {"csv_content": content, "source_bucket": b}
        except Exception:
            continue
    raise HTTPException(status_code=404, detail="Problem statements CSV has not been uploaded yet.")


@app.get("/api/problems/csv")
def get_problems_csv_download_url():
    """
    Generates a presigned S3 GET URL so the frontend can fetch the
    problem statements CSV directly from S3.
    """
    try:
        settings_res = table.get_item(Key={'TeamID': 'SYSTEM_SETTINGS'})
        settings_item = settings_res.get('Item', {})
        if not settings_item.get('ProblemsCsvUploaded', False):
            raise HTTPException(
                status_code=404,
                detail="Problem statements CSV has not been uploaded yet."
            )
        presigned_url = s3_client.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': S3_BUCKET,
                'Key': PROBLEMS_CSV_S3_KEY
            },
            ExpiresIn=3600
        )
        return {"presigned_url": presigned_url}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.post("/api/problems/reset")
@app.delete("/api/problems/reset")
@app.post("/api/problems/reset-csv")
@app.delete("/api/problems/reset-csv")
def reset_problems_csv():
    """
    Admin-only: Deletes Problem Statements CSV from all target S3 buckets,
    marks ProblemsCsvUploaded = False, and disables SelectionEnabled.
    """
    deleted_buckets = []
    for b in get_target_s3_buckets():
        try:
            s3_client.delete_object(
                Bucket=b,
                Key=PROBLEMS_CSV_S3_KEY
            )
            deleted_buckets.append(b)
        except Exception as e:
            print(f"Notice: S3 delete from bucket {b} failed: {e}")

    try:
        table.update_item(
            Key={'TeamID': 'SYSTEM_SETTINGS'},
            UpdateExpression="set ProblemsCsvUploaded = :val, SelectionEnabled = :sel, ProblemsData = :empty",
            ExpressionAttributeValues={':val': False, ':sel': False, ':empty': []}
        )
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])

    invalidate_problems_cache()
    return {
        "message": "Problem statements CSV successfully deleted from S3 and selection gate reset.",
        "deleted_buckets": deleted_buckets
    }


@app.post("/teams/{team_id}/submit-link")
def submit_deployed_link(team_id: str, req: DeployedLinkRequest):
    if team_id == "SYSTEM_SETTINGS":
        raise HTTPException(status_code=400, detail="Invalid team request")
    try:
        link = req.deployed_link.strip()
        if not link.startswith("http://") and not link.startswith("https://"):
            raise HTTPException(status_code=400, detail="Invalid URL format. Must start with http:// or https://")
        
        table.update_item(
            Key={'TeamID': team_id},
            UpdateExpression="set DeployedLink = :val, LinkSubmittedAt = :ts",
            ExpressionAttributeValues={
                ':val': link,
                ':ts': int(time.time())
            }
        )
        return {"message": "Deployed link submitted successfully.", "deployed_link": link}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.post("/teams/{team_id}/submit-review")
def submit_team_review(team_id: str, req: TeamReviewRequest):
    if team_id == "SYSTEM_SETTINGS":
        raise HTTPException(status_code=400, detail="Invalid team review request")
    try:
        table.update_item(
            Key={'TeamID': team_id},
            UpdateExpression="set EvaluationStatus = :status, ReviewFeedback = :feedback, EvaluationScore = :score, TotalScore = :score, TotalMarks = :score, Score = :score",
            ExpressionAttributeValues={
                ':status': req.status,
                ':feedback': req.feedback,
                ':score': req.score
            }
        )
        return {"message": "Review submitted successfully."}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.post("/settings/toggle-delete-protection")
def toggle_delete_protection(req: ToggleDeleteProtectionRequest):
    try:
        table.update_item(
            Key={'TeamID': 'SYSTEM_SETTINGS'},
            UpdateExpression="set DeleteProtectionActive = :val",
            ExpressionAttributeValues={':val': req.enabled}
        )
        return {"message": "Delete protection configuration updated successfully.", "enabled": req.enabled}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.post("/settings/toggle-feedback")
def toggle_feedback(req: ToggleFeedbackRequest):
    """
    Admin-only: Globally enables or disables the participant feedback form.
    Controls the FeedbackEnabled boolean on the SYSTEM_SETTINGS item.
    """
    try:
        table.update_item(
            Key={'TeamID': 'SYSTEM_SETTINGS'},
            UpdateExpression="set FeedbackEnabled = :val",
            ExpressionAttributeValues={':val': req.enabled}
        )
        return {"message": "Feedback gate updated successfully.", "enabled": req.enabled}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.post("/teams/{team_id}/submit-feedback")
def submit_feedback(team_id: str, req: FeedbackSubmissionRequest):
    """
    Participant endpoint: Submits event feedback for a specific team member.
    Guards:
      - FeedbackEnabled must be True in SYSTEM_SETTINGS.
      - The team and member (by reg_no) must exist.
    On success, sets FeedbackSubmitted = True on the member's map inside Members[].
    Uses a read-modify-write because DynamoDB cannot update a list element by predicate.
    """
    if team_id == "SYSTEM_SETTINGS":
        raise HTTPException(status_code=400, detail="Invalid team feedback request")

    # Validate rating range
    if req.rating < 1 or req.rating > 5:
        raise HTTPException(status_code=400, detail="Rating must be between 1 and 5.")

    try:
        # Gate: feedback must be globally enabled
        settings_res = table.get_item(Key={'TeamID': 'SYSTEM_SETTINGS'})
        settings_item = settings_res.get('Item', {})
        if not settings_item.get('FeedbackEnabled', False):
            raise HTTPException(
                status_code=403,
                detail="Feedback submission is currently disabled by the administrator."
            )

        # Fetch the team record
        team_res = table.get_item(Key={'TeamID': team_id})
        team = team_res.get('Item')
        if not team:
            raise HTTPException(status_code=404, detail="Team not found")

        members = team.get('Members', [])
        member_found = False
        updated_members = []

        for m in members:
            # Normalize reg_no comparison (case-insensitive, stripped)
            if m.get('regNo', '').strip().lower() == req.reg_no.strip().lower():
                m = dict(m)  # make a mutable copy
                m['FeedbackSubmitted'] = True
                m['FeedbackData'] = {
                    'HowWasEvent': req.how_was_event,
                    'Improvements': req.improvements,
                    'Discomfort': req.discomfort,
                    'Other': req.other,
                    'Rating': req.rating
                }
                member_found = True
            updated_members.append(m)

        if not member_found:
            raise HTTPException(
                status_code=404,
                detail=f"Member with registration number '{req.reg_no}' not found in team '{team_id}'."
            )

        # Write back the full Members list
        table.update_item(
            Key={'TeamID': team_id},
            UpdateExpression="SET Members = :members",
            ExpressionAttributeValues={':members': updated_members}
        )

        return {"message": "Feedback submitted successfully. Certificate download is now unlocked."}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.get("/api/certificates/participation/presigned-url")
def get_participation_certificate_url(team_id: str, reg_no: str):
    """
    Participant endpoint: Returns a presigned S3 GET URL for a participation certificate.
    S3 key format: participation-certificates/{team_id}/{reg_no}.pdf
    Guards:
      - Team must exist.
      - Member (by reg_no) must have FeedbackSubmitted == True.
    """
    if team_id == "SYSTEM_SETTINGS":
        raise HTTPException(status_code=400, detail="Invalid team request")

    try:
        team_res = table.get_item(Key={'TeamID': team_id})
        team = team_res.get('Item')
        if not team:
            raise HTTPException(status_code=404, detail="Team not found")

        members = team.get('Members', [])
        member = None
        for m in members:
            if m.get('regNo', '').strip().lower() == reg_no.strip().lower():
                member = m
                break

        if not member:
            raise HTTPException(
                status_code=404,
                detail=f"Member with registration number '{reg_no}' not found."
            )

        if not member.get('FeedbackSubmitted', False):
            raise HTTPException(
                status_code=403,
                detail="Certificate download is locked until feedback is submitted."
            )

        # Build the exact S3 key per specification
        s3_key = f"participation-certificates/{team_id}/{reg_no}.pdf"

        presigned_url = s3_client.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': S3_BUCKET,
                'Key': s3_key
            },
            ExpiresIn=3600
        )

        return {"url": presigned_url, "s3_key": s3_key}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.post("/admin/import-data")
def import_data(req: ImportRequest):
    try:
        settings_res = table.get_item(Key={'TeamID': 'SYSTEM_SETTINGS'})
        settings_item = settings_res.get('Item', {})
        if settings_item.get('DeleteProtectionActive', False):
            raise HTTPException(
                status_code=400, 
                detail="Action Denied: Delete Protection is currently active. Disable it to modify/overwrite team registers."
            )

        team_members_map = {}
        for p in req.participants:
            t_id = p.TeamId.strip()
            if t_id not in team_members_map:
                team_members_map[t_id] = []
            team_members_map[t_id].append({
                "name": p.Name,
                "regNo": p.RegNo,
                "email": p.Email,
                "phone": p.Phone,
                "gender": p.Gender,
                "branch": p.Branch,
                "year": p.Year,
                "accommodation": p.Accommodation,
                "hostelName": p.HostelName,
                "roomNo": p.RoomNo,
                "wardenName": p.WardenName,
                "wardenPhone": p.WardenPhone
            })

        imported_count = 0
        for team in req.teams:
            t_id = team.TeamID.strip()
            members = team_members_map.get(t_id, [])
            
            existing_problem = None
            existing_link = None
            existing_certs = {}
            existing_status = None
            existing_feedback = None
            existing_score = None
            
            try:
                existing_res = table.get_item(Key={'TeamID': t_id})
                existing_item = existing_res.get('Item')
                if existing_item:
                    existing_problem = existing_item.get('SelectedProblem')
                    existing_link = existing_item.get('DeployedLink')
                    existing_certs = existing_item.get('Certificates', {})
                    existing_status = existing_item.get('EvaluationStatus')
                    existing_feedback = existing_item.get('ReviewFeedback')
                    existing_score = existing_item.get('EvaluationScore')
            except Exception:
                pass

            item_payload = {
                'TeamID': t_id,
                'Team Name': team.TeamName,
                'Password': team.Password,
                'Leader Name': team.LeaderName,
                'Leader Email': team.LeaderEmail,
                'Leader Phone': team.LeaderPhone,
                'Leader RegNo': team.LeaderRegNo,
                'Transaction ID': team.TransactionID,
                'Status': team.Status,
                'Submitted At': team.SubmittedAt,
                'Members': members
            }

            if existing_problem:
                item_payload['SelectedProblem'] = existing_problem
            if existing_link:
                item_payload['DeployedLink'] = existing_link
            if existing_certs:
                item_payload['Certificates'] = existing_certs
            if existing_status:
                item_payload['EvaluationStatus'] = existing_status
            if existing_feedback:
                item_payload['ReviewFeedback'] = existing_feedback
            if existing_score:
                item_payload['EvaluationScore'] = existing_score

            table.put_item(Item=item_payload)
            imported_count += 1

        return {"message": f"Successfully imported {imported_count} team profiles and their roster members."}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.post("/seed-csv-data")
@app.post("/admin/seed-csv-data")
async def seed_csv_data(request: Request):
    """
    Ingests and seeds parsed teams and participants CSV records into DynamoDB.
    Handles varied CSV column names (TeamID / TeamId, Team Name / TeamName, etc.)
    and preserves existing team evaluations, link submissions, and problem selections.
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body.")

    raw_teams = body.get("teams", [])
    raw_participants = body.get("participants", [])

    if not raw_teams and not raw_participants:
        raise HTTPException(status_code=400, detail="No teams or participants provided in payload.")

    try:
        settings_res = table.get_item(Key={'TeamID': 'SYSTEM_SETTINGS'})
        settings_item = settings_res.get('Item', {})
        if settings_item.get('DeleteProtectionActive', False):
            raise HTTPException(
                status_code=400,
                detail="Action Denied: Delete Protection is currently active. Disable it to modify/overwrite team registers."
            )

        # 1. Group participants by Team ID
        team_members_map = {}
        for p in raw_participants:
            t_id = str(p.get("TeamID") or p.get("TeamId") or p.get("team_id") or p.get("teamId") or "").strip()
            if not t_id:
                continue
            if t_id not in team_members_map:
                team_members_map[t_id] = []

            team_members_map[t_id].append({
                "name": str(p.get("Name") or p.get("name") or "").strip(),
                "regNo": str(p.get("RegNo") or p.get("regNo") or p.get("reg_no") or "").strip(),
                "email": str(p.get("Email") or p.get("email") or "").strip(),
                "phone": str(p.get("Phone") or p.get("phone") or "").strip(),
                "gender": str(p.get("Gender") or p.get("gender") or "").strip(),
                "branch": str(p.get("Branch") or p.get("branch") or "").strip(),
                "year": str(p.get("Year") or p.get("year") or "").strip(),
                "accommodation": str(p.get("Accommodation") or p.get("accommodation") or "").strip(),
                "hostelName": str(p.get("HostelName") or p.get("hostelName") or p.get("Hostel") or "").strip(),
                "roomNo": str(p.get("RoomNo") or p.get("roomNo") or p.get("Room") or "").strip(),
                "wardenName": str(p.get("WardenName") or p.get("wardenName") or "").strip(),
                "wardenPhone": str(p.get("WardenPhone") or p.get("wardenPhone") or "").strip(),
            })

        # 2. Build teams dict
        teams_to_seed = {}

        # Add explicit teams if provided
        for team in raw_teams:
            t_id = str(team.get("TeamID") or team.get("TeamId") or team.get("team_id") or team.get("teamId") or "").strip()
            if not t_id:
                continue
            t_name = str(team.get("Team Name") or team.get("TeamName") or team.get("team_name") or f"Team {t_id}").strip()
            password = str(team.get("Password") or team.get("password") or "hackodyssey2026").strip()
            leader_name = str(team.get("Leader Name") or team.get("LeaderName") or team.get("leader_name") or "").strip()
            leader_email = str(team.get("Leader Email") or team.get("LeaderEmail") or team.get("leader_email") or "").strip()
            leader_phone = str(team.get("Leader Phone") or team.get("LeaderPhone") or team.get("leader_phone") or "").strip()
            leader_reg_no = str(team.get("Leader RegNo") or team.get("LeaderRegNo") or team.get("leader_reg_no") or "").strip()
            status = str(team.get("Status") or team.get("TransactionStatus") or "SUCCESS").strip()
            submitted_at = str(team.get("Submitted At") or team.get("SubmittedAt") or team.get("SubmittedTimestamp") or "").strip()
            transaction_id = str(team.get("Transaction ID") or team.get("TransactionID") or "").strip()

            teams_to_seed[t_id] = {
                'TeamID': t_id,
                'Team Name': t_name,
                'Password': password,
                'Leader Name': leader_name,
                'Leader Email': leader_email,
                'Leader Phone': leader_phone,
                'Leader RegNo': leader_reg_no,
                'Transaction ID': transaction_id,
                'Status': status,
                'Submitted At': submitted_at,
                'Members': team_members_map.get(t_id, [])
            }

        # If participants exist for teams not explicitly in raw_teams, synthesize team records
        for t_id, members in team_members_map.items():
            if t_id not in teams_to_seed:
                matching_p = next((p for p in raw_participants if str(p.get("TeamID") or p.get("TeamId") or p.get("team_id") or "").strip() == t_id), {})
                t_name = str(matching_p.get("TeamName") or matching_p.get("Team Name") or f"Team {t_id}").strip()
                pwd = str(matching_p.get("Password") or matching_p.get("password") or "hackodyssey2026").strip()
                status = str(matching_p.get("TransactionStatus") or matching_p.get("Status") or "SUCCESS").strip()
                sub_at = str(matching_p.get("SubmittedTimestamp") or matching_p.get("SubmittedAt") or "").strip()
                selected_prob = str(matching_p.get("SelectedProblem") or "").strip()

                first_m = members[0] if members else {}
                team_record = {
                    'TeamID': t_id,
                    'Team Name': t_name,
                    'Password': pwd,
                    'Leader Name': first_m.get('name', ''),
                    'Leader Email': first_m.get('email', ''),
                    'Leader Phone': first_m.get('phone', ''),
                    'Leader RegNo': first_m.get('regNo', ''),
                    'Transaction ID': '',
                    'Status': status,
                    'Submitted At': sub_at,
                    'Members': members
                }
                if selected_prob:
                    team_record['SelectedProblem'] = selected_prob
                teams_to_seed[t_id] = team_record

        # 3. For each team, resolve leader info from members if blank, preserve existing items, and save
        imported_count = 0
        for t_id, item_payload in teams_to_seed.items():
            members = item_payload.get('Members', [])
            if not item_payload['Leader Name'] and members:
                item_payload['Leader Name'] = members[0].get('name', '')
                item_payload['Leader Email'] = members[0].get('email', '')
                item_payload['Leader Phone'] = members[0].get('phone', '')
                item_payload['Leader RegNo'] = members[0].get('regNo', '')

            try:
                existing_res = table.get_item(Key={'TeamID': t_id})
                existing_item = existing_res.get('Item')
                if existing_item:
                    for field in ['SelectedProblem', 'DeployedLink', 'Certificates', 'EvaluationStatus', 'ReviewFeedback', 'EvaluationScore']:
                        if field in existing_item and field not in item_payload:
                            item_payload[field] = existing_item[field]
            except Exception:
                pass

            table.put_item(Item=item_payload)
            imported_count += 1

        # Backup seeded teams and participants directly to AWS S3 for persistent redundant cloud storage
        try:
            backup_teams_to_s3(list(teams_to_seed.values()))
            backup_participants_to_s3(raw_participants)
        except Exception as bkp_err:
            print(f"Notice during cloud S3 backup: {bkp_err}")

        return {
            "message": f"Successfully seeded {imported_count} team profiles and {len(raw_participants)} participant records into DynamoDB.",
            "seeded_teams": imported_count,
            "seeded_participants": len(raw_participants)
        }
    except HTTPException:
        raise
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response.get('Error', {}).get('Message', str(e)))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/admin/delete-all-teams")
@app.delete("/admin/delete-all-teams")
@app.post("/teams/purge-all-data")
@app.delete("/teams/purge-all-data")
def delete_all_teams(req: DeleteAllRequest):
    if req.password != "delete":
        raise HTTPException(status_code=403, detail="Unauthorized: Incorrect deletion authorization key.")
    try:
        settings_res = table.get_item(Key={'TeamID': 'SYSTEM_SETTINGS'})
        settings_item = settings_res.get('Item', {})
        if settings_item.get('DeleteProtectionActive', False):
            raise HTTPException(
                status_code=400,
                detail="Action Denied: Delete Protection is currently active. Disable the Data Lock before purging records."
            )

        response = table.scan()
        items = response.get('Items', [])
        deleted_count = 0

        for item in items:
            team_id = item.get('TeamID')
            if team_id == 'SYSTEM_SETTINGS':
                continue
            table.delete_item(Key={'TeamID': team_id})
            deleted_count += 1

        # Cleanly purge all jury review scores and legacy scores in SYSTEM_SETTINGS
        table.update_item(
            Key={'TeamID': 'SYSTEM_SETTINGS'},
            UpdateExpression="SET TeamReviewScores = :empty, JuryScores = :empty",
            ExpressionAttributeValues={':empty': {}}
        )

        return {"message": f"Successfully purged {deleted_count} team records and all jury scores from the database."}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.post("/admin/reset-jury-scores")
@app.post("/jury/reset-scores")
def reset_all_jury_scores():
    """Resets all jury evaluations and marks across the system."""
    try:
        table.update_item(
            Key={'TeamID': 'SYSTEM_SETTINGS'},
            UpdateExpression="SET TeamReviewScores = :empty, JuryScores = :empty",
            ExpressionAttributeValues={':empty': {}}
        )
        return {"message": "All jury scores have been successfully reset."}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.post("/admin/initialize-teams", status_code=201)
def initialize_teams():
    # 1. Write System Settings Record
    system_settings = {
        "TeamID": "SYSTEM_SETTINGS",
        "SelectionEnabled": 1,
        "TimerLaunched": 1,
        "TimerStartTime": 0,
        "TimerDuration": 0
    }
    
    try:
        table.put_item(Item=system_settings)
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Failed to write system settings: {str(e)}")

    # 2. Process CSV and group participants by TeamID
    csv_file_path = "euphoria26_participants.csv"
    if not os.path.exists(csv_file_path):
        sample_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "sample_data", "dummy_participants.csv")
        if os.path.exists(sample_path):
            csv_file_path = sample_path
        else:
            return {"message": "System settings initialized in DynamoDB. Seed teams and participants via /seed-csv-data."}

    teams_data = {}
    
    try:
        with open(csv_file_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                team_id = row.get("TeamID")
                if not team_id:
                    continue
                
                if team_id not in teams_data:
                    teams_data[team_id] = {
                        "TeamID": team_id,
                        "TeamName": row.get("TeamName", f"Team {team_id}"),
                        "Password": row.get("Password", "default_password"),
                        "TransactionStatus": row.get("TransactionStatus", "SUCCESS"),
                        "SubmittedTimestamp": row.get("SubmittedTimestamp", ""),
                        "SelectedProblem": row.get("SelectedProblem", ""),
                        "Participants": []
                    }
                
                participant = {
                    "RegNo": row.get("RegNo", ""),
                    "Name": row.get("Name", ""),
                    "Email": row.get("Email", ""),
                    "Phone": row.get("Phone", ""),
                    "Certificates": []
                }
                teams_data[team_id]["Participants"].append(participant)
                
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading CSV: {str(e)}")

    # 3. Batch write teams to DynamoDB
    try:
        with table.batch_writer() as batch:
            for team_id, team_record in teams_data.items():
                batch.put_item(Item=team_record)
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Failed to batch write teams: {str(e)}")
        
    return {"message": "Successfully initialized system settings and teams."}

# ─────────────────────────────────────────────────────────────────────────────
# Problem Statements: list (S3 CSV + inline adds)
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_sdg_id(raw: str) -> str:
    """Normalise sdg_id; handles casing, spaces, dashes, and semantic keywords."""
    normalised = (raw or '').strip().upper().replace(" ", "").replace("-", "").replace("_", "")
    if normalised in VALID_SDG_IDS:
        return normalised
    mapping = {
        '2': 'SDG2', 'HUNGER': 'SDG2', 'AGRICULTURE': 'SDG2', 'AGRI': 'SDG2',
        '3': 'SDG3', 'HEALTH': 'SDG3', 'WELLBEING': 'SDG3', 'MEDICAL': 'SDG3',
        '4': 'SDG4', 'EDUCATION': 'SDG4', 'LEARNING': 'SDG4',
        '6': 'SDG6', 'WATER': 'SDG6', 'SANITATION': 'SDG6',
        '11': 'SDG11', 'CITIES': 'SDG11', 'COMMUNITIES': 'SDG11', 'URBAN': 'SDG11',
        '13': 'SDG13', 'CLIMATE': 'SDG13', 'ENVIRONMENT': 'SDG13',
        'HW': 'HARDWARE', 'HARDWARE': 'HARDWARE', 'IOT': 'HARDWARE', 'ROBOTICS': 'HARDWARE'
    }
    for k, v in mapping.items():
        if k in normalised:
            return v
    return normalised if normalised in VALID_SDG_IDS else 'HARDWARE'


def _parse_problems_csv(csv_text: str) -> list:
    """Parse raw CSV text into a list of problem-statement dicts with category."""
    import io
    if not csv_text or not csv_text.strip():
        return []
    # Strip UTF-8 BOM if present
    if csv_text.startswith('\ufeff'):
        csv_text = csv_text[1:]
    reader = csv.DictReader(io.StringIO(csv_text))
    problems = []
    for row in reader:
        if not row:
            continue
        clean_row = {str(k).strip().lower().replace(' ', '_').replace('-', '_'): (str(v).strip() if v is not None else '') for k, v in row.items() if k is not None}
        
        sdg_raw = (
            clean_row.get('sdg_id') or clean_row.get('sdgid') or clean_row.get('sdg') or 
            clean_row.get('category') or clean_row.get('domain') or clean_row.get('track') or ''
        )
        sdg_id = _resolve_sdg_id(sdg_raw)
        
        prob_id = (
            clean_row.get('problem_id') or clean_row.get('problemid') or clean_row.get('id') or 
            clean_row.get('code') or clean_row.get('track_id') or ''
        )
        
        title = (
            clean_row.get('title') or clean_row.get('problem_title') or clean_row.get('problemtitle') or 
            clean_row.get('name') or ''
        )
        
        desc = (
            clean_row.get('description') or clean_row.get('problem_description') or 
            clean_row.get('problemdescription') or clean_row.get('desc') or clean_row.get('details') or ''
        )
        
        reqs = (
            clean_row.get('requirements') or clean_row.get('requirement') or clean_row.get('tech_stack') or 
            clean_row.get('stack') or clean_row.get('prerequisites') or ''
        )
        
        exps = (
            clean_row.get('expectations') or clean_row.get('expectation') or clean_row.get('deliverables') or 
            clean_row.get('outcomes') or ''
        )
        
        if prob_id or title:
            problems.append({
                'problem_id':   prob_id,
                'sdg_id':       sdg_id,
                'category':     SDG_CATEGORY_LABELS.get(sdg_id, 'Hardware'),
                'title':        title,
                'description':  desc,
                'requirements': reqs,
                'expectations': exps,
            })
    return problems


# ── Problems In-Memory Cache (3s TTL for rapid propagation) ───────────────────
_PROBLEMS_CACHE = {
    "data": None,
    "timestamp": 0.0
}
_PROBLEMS_CACHE_TTL = 3.0  # 3 seconds cache

def invalidate_problems_cache():
    global _PROBLEMS_CACHE
    _PROBLEMS_CACHE["data"] = None
    _PROBLEMS_CACHE["timestamp"] = 0.0


@app.get("/api/problems/list")
def list_problems():
    """
    Returns merged problem statements:
    1. Problems from S3 CSV (checking euphoria26-certificates, S3_BUCKET, and target buckets)
    2. Inline-added problems stored in SYSTEM_SETTINGS.ProblemsData
    Categorised by sdg_id at response time.
    """
    global _PROBLEMS_CACHE
    now = time.time()
    if _PROBLEMS_CACHE["data"] is not None and (now - _PROBLEMS_CACHE["timestamp"] < _PROBLEMS_CACHE_TTL):
        return _PROBLEMS_CACHE["data"]

    is_uploaded = True
    inline = []
    try:
        settings_res = table.get_item(Key={'TeamID': 'SYSTEM_SETTINGS'})
        settings_item = settings_res.get('Item', {})
        is_uploaded = bool(settings_item.get('ProblemsCsvUploaded', False))
        inline = settings_item.get('ProblemsData', [])
    except Exception as e:
        print(f"Notice: Checking DynamoDB settings for problems: {e}")

    problems = []

    # 1. From S3 CSV if marked as uploaded or exists
    if is_uploaded:
        csv_text = None
        for b in get_target_s3_buckets():
            try:
                resp = s3_client.get_object(Bucket=b, Key=PROBLEMS_CSV_S3_KEY)
                text = resp['Body'].read().decode('utf-8')
                if text and text.strip():
                    csv_text = text
                    break
            except Exception:
                continue

        if csv_text:
            problems.extend(_parse_problems_csv(csv_text))

    # 2. From inline adds stored in DynamoDB SYSTEM_SETTINGS
    if inline:
        csv_ids = {p.get('problem_id') for p in problems if p.get('problem_id')}
        for p in inline:
            if p.get('problem_id') not in csv_ids:
                problems.append(p)

    result = {'count': len(problems), 'problems': problems}
    _PROBLEMS_CACHE["data"] = result
    _PROBLEMS_CACHE["timestamp"] = now
    return result


@app.post("/api/problems/add")
def add_problem_inline(req: ProblemStatementItem):
    """
    Admin: Add a single problem statement inline (stored in SYSTEM_SETTINGS.ProblemsData).
    Auto-assigns category based on sdg_id.
    """
    if not req.problem_id.strip():
        raise HTTPException(status_code=400, detail="problem_id is required.")
    if not req.title.strip():
        raise HTTPException(status_code=400, detail="title is required.")

    sdg_id = _resolve_sdg_id(req.sdg_id)
    new_entry = {
        'problem_id':   req.problem_id.strip(),
        'sdg_id':       sdg_id,
        'category':     SDG_CATEGORY_LABELS.get(sdg_id, 'Hardware'),
        'title':        req.title.strip(),
        'description':  req.description.strip(),
        'requirements': req.requirements.strip(),
        'expectations': req.expectations.strip(),
    }

    try:
        table.update_item(
            Key={'TeamID': 'SYSTEM_SETTINGS'},
            UpdateExpression='SET ProblemsData = if_not_exists(ProblemsData, :empty)',
            ExpressionAttributeValues={':empty': []}
        )
        settings_res = table.get_item(Key={'TeamID': 'SYSTEM_SETTINGS'})
        current = list(settings_res.get('Item', {}).get('ProblemsData', []))
        current = [p for p in current if p.get('problem_id') != new_entry['problem_id']]
        current.append(new_entry)
        table.update_item(
            Key={'TeamID': 'SYSTEM_SETTINGS'},
            UpdateExpression='SET ProblemsData = :val',
            ExpressionAttributeValues={':val': current}
        )
        invalidate_problems_cache()
        return {'message': 'Problem statement added successfully.', 'problem': new_entry}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.post("/api/problems/seed-default")
def seed_default_problems_to_s3():
    """
    Seeds default problem statements directly into AWS S3 at problemstatements/problems.csv.
    Ensures S3 has canonical problem statements for the hackathon across all target buckets.
    """
    sample_csv = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "sample_data", "dummy_problems.csv")
    if not os.path.exists(sample_csv):
        raise HTTPException(status_code=404, detail="Default problems CSV template not found on server.")
    
    try:
        with open(sample_csv, "r", encoding="utf-8") as f:
            csv_content = f.read()

        saved_buckets = []
        for b in get_target_s3_buckets():
            try:
                s3_client.put_object(
                    Bucket=b,
                    Key=PROBLEMS_CSV_S3_KEY,
                    Body=csv_content.encode('utf-8'),
                    ContentType='text/csv'
                )
                saved_buckets.append(b)
            except Exception as e:
                print(f"Seed to bucket {b} failed: {e}")

        table.update_item(
            Key={'TeamID': 'SYSTEM_SETTINGS'},
            UpdateExpression="set ProblemsCsvUploaded = :val",
            ExpressionAttributeValues={':val': True}
        )
        invalidate_problems_cache()
        return {
            "message": "Default problem statements seeded successfully into AWS S3.",
            "s3_buckets": saved_buckets,
            "s3_key": PROBLEMS_CSV_S3_KEY
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────────────────────────────────────
# Team–Problem Assignment (admin assigns a problem to a team)
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/api/teams/{team_id}/assign-problem")
def assign_problem_to_team(team_id: str, req: AssignProblemRequest):
    """
    Admin: Assign or unassign a problem statement to a specific team.
    Writes AdminAssignedProblem, AdminAssignedProblemTitle, AdminAssignedSdgId
    to the team's DynamoDB record (separate from SelectedProblem).
    Passing problem_id=null or omitting it = unassign.
    """
    if team_id == 'SYSTEM_SETTINGS':
        raise HTTPException(status_code=400, detail='Invalid team ID.')
    try:
        res = table.get_item(Key={'TeamID': team_id})
        if not res.get('Item'):
            raise HTTPException(status_code=404, detail='Team not found.')

        if req.problem_id:  # Assign
            problem_title = (req.problem_title or '').strip()
            if problem_title:
                scan_resp = table.scan()
                all_teams = scan_resp.get('Items', [])
                count = sum(
                    1 for t in all_teams 
                    if t.get('TeamID') not in ('SYSTEM_SETTINGS', team_id) 
                    and (t.get('SelectedProblem') == problem_title or t.get('AdminAssignedProblem') == req.problem_id.strip())
                )
                if count >= 3:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Problem statement has reached maximum capacity of 3 teams."
                    )

            sdg_id = _resolve_sdg_id(req.sdg_id or '')
            table.update_item(
                Key={'TeamID': team_id},
                UpdateExpression=(
                    'SET AdminAssignedProblem = :pid, '
                    'AdminAssignedProblemTitle = :title, '
                    'AdminAssignedSdgId = :sdg, '
                    'SelectedProblem = :title'
                ),
                ExpressionAttributeValues={
                    ':pid':   req.problem_id.strip(),
                    ':title': problem_title,
                    ':sdg':   sdg_id,
                }
            )
            return {'message': 'Problem assigned successfully.', 'team_id': team_id, 'problem_id': req.problem_id}
        else:  # Unassign
            table.update_item(
                Key={'TeamID': team_id},
                UpdateExpression='REMOVE AdminAssignedProblem, AdminAssignedProblemTitle, AdminAssignedSdgId, SelectedProblem'
            )
            return {'message': 'Problem unassigned successfully.', 'team_id': team_id}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


# ─────────────────────────────────────────────────────────────────────────────
# Jury Portal: login + scoring
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/jury/login")
def jury_login(req: JuryLoginRequest):
    """Validates jury credentials. Returns juror_id on success."""
    expected = JURY_CREDENTIALS.get(req.username.strip().lower())
    if not expected or req.password != expected:
        raise HTTPException(status_code=401, detail='Invalid jury credentials.')
    return {'ok': True, 'juror_id': req.username.strip().lower()}


@app.post("/jury/score")
def submit_jury_score(req: JuryScoreSubmit):
    """
    Submit or update a jury score for a team for Review 1 or Review 2.
    Scores are stored in SYSTEM_SETTINGS.TeamReviewScores keyed by team_id,
    ensuring that when one juror allocates marks, it syncs across all juror portals.
    Total score for a team is computed out of 200 (Review 1 max 100 + Review 2 max 100).
    """
    if req.juror_id not in JURY_CREDENTIALS:
        raise HTTPException(status_code=401, detail='Invalid juror ID.')

    for field_name, val in [
        ('innovation', req.innovation),
        ('execution', req.execution),
        ('impact', req.impact),
        ('presentation', req.presentation),
    ]:
        if not (0 <= val <= 25):
            raise HTTPException(status_code=400, detail=f'{field_name} must be between 0 and 25.')

    round_key = req.review_round.strip().lower() if getattr(req, 'review_round', None) else "review1"
    if round_key not in ("review1", "review2"):
        round_key = "review1"

    total = req.innovation + req.execution + req.impact + req.presentation
    score_entry = {
        'innovation':   req.innovation,
        'execution':    req.execution,
        'impact':       req.impact,
        'presentation': req.presentation,
        'total':        total,
        'submitted_at': int(time.time()),
        'juror_id':     req.juror_id,
        'team_id':      req.team_id,
        'review_round': round_key,
    }

    try:
        # Fetch current SYSTEM_SETTINGS to retrieve TeamReviewScores
        settings_res = table.get_item(Key={'TeamID': 'SYSTEM_SETTINGS'})
        settings = settings_res.get('Item', {})
        team_reviews = settings.get('TeamReviewScores', {})
        
        current_team_rec = team_reviews.get(req.team_id, {
            'team_id': req.team_id,
            'review1': None,
            'review2': None,
        })

        # Save this review round
        current_team_rec[round_key] = score_entry

        r1_val = current_team_rec.get('review1', {}).get('total', 0) if current_team_rec.get('review1') else 0
        r2_val = current_team_rec.get('review2', {}).get('total', 0) if current_team_rec.get('review2') else 0
        total_for_200 = r1_val + r2_val

        current_team_rec['total_score'] = total_for_200
        current_team_rec['r1_total'] = r1_val
        current_team_rec['r2_total'] = r2_val
        current_team_rec['last_updated_at'] = int(time.time())
        current_team_rec['last_juror_id'] = req.juror_id

        # Update TeamReviewScores map in DynamoDB
        table.update_item(
            Key={'TeamID': 'SYSTEM_SETTINGS'},
            UpdateExpression='SET TeamReviewScores = if_not_exists(TeamReviewScores, :empty)',
            ExpressionAttributeValues={':empty': {}}
        )
        table.update_item(
            Key={'TeamID': 'SYSTEM_SETTINGS'},
            UpdateExpression='SET TeamReviewScores.#tid = :team_rec',
            ExpressionAttributeNames={'#tid': req.team_id},
            ExpressionAttributeValues={':team_rec': current_team_rec}
        )

        # Legacy backward-compatibility in JuryScores
        legacy_key = f"{req.juror_id}_{req.team_id}_{round_key}"
        table.update_item(
            Key={'TeamID': 'SYSTEM_SETTINGS'},
            UpdateExpression='SET JuryScores = if_not_exists(JuryScores, :empty)',
            ExpressionAttributeValues={':empty': {}}
        )
        table.update_item(
            Key={'TeamID': 'SYSTEM_SETTINGS'},
            UpdateExpression='SET JuryScores.#k = :val',
            ExpressionAttributeNames={'#k': legacy_key},
            ExpressionAttributeValues={':val': score_entry}
        )

        # Store all marks fields directly onto the team's item in DynamoDB
        try:
            update_parts = [
                "EvaluationScore = :total",
                "TotalScore = :total",
                "TotalMarks = :total",
                "Score = :total",
                "Review1Score = :r1_tot",
                "Review2Score = :r2_tot",
                "ReviewScores = :rs",
                "EvaluationStatus = :eval_status",
                "LastEvaluatedAt = :ts",
                "LastJurorID = :juror"
            ]
            expr_vals = {
                ':total': total_for_200,
                ':r1_tot': r1_val,
                ':r2_tot': r2_val,
                ':rs': current_team_rec,
                ':eval_status': 'EVALUATED',
                ':ts': int(time.time()),
                ':juror': req.juror_id
            }

            if round_key == "review1":
                update_parts.extend([
                    "Review1_Innovation = :r1_inno",
                    "Review1_Execution = :r1_exec",
                    "Review1_Impact = :r1_imp",
                    "Review1_Presentation = :r1_pres",
                    "Review1_Total = :r1_tot"
                ])
                expr_vals[':r1_inno'] = req.innovation
                expr_vals[':r1_exec'] = req.execution
                expr_vals[':r1_imp'] = req.impact
                expr_vals[':r1_pres'] = req.presentation
            elif round_key == "review2":
                update_parts.extend([
                    "Review2_Innovation = :r2_inno",
                    "Review2_Execution = :r2_exec",
                    "Review2_Impact = :r2_imp",
                    "Review2_Presentation = :r2_pres",
                    "Review2_Total = :r2_tot"
                ])
                expr_vals[':r2_inno'] = req.innovation
                expr_vals[':r2_exec'] = req.execution
                expr_vals[':r2_imp'] = req.impact
                expr_vals[':r2_pres'] = req.presentation

            table.update_item(
                Key={'TeamID': req.team_id},
                UpdateExpression="SET " + ", ".join(update_parts),
                ExpressionAttributeValues=expr_vals
            )
        except Exception as team_upd_err:
            print(f"Notice updating team item review score: {team_upd_err}")

        # Backup marks to AWS S3
        backup_marks_to_s3(team_reviews)

        return {
            'message': f'Marks for {round_key.upper()} submitted successfully.',
            'score': score_entry,
            'team_scores': current_team_rec
        }
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.get("/jury/scores")
def get_jury_scores():
    """Returns all team review scores synchronized across all jury portals."""
    try:
        settings_res = table.get_item(Key={'TeamID': 'SYSTEM_SETTINGS'})
        settings = settings_res.get('Item', {})
        team_reviews = settings.get('TeamReviewScores', {})
        legacy_scores = settings.get('JuryScores', {})
        return {
            'scores': team_reviews,
            'team_reviews': team_reviews,
            'legacy_scores': legacy_scores
        }
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


# ─────────────────────────────────────────────────────────────────────────────
# Leaderboard
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/settings/toggle-leaderboard")
def toggle_leaderboard(req: ToggleLeaderboardRequest):
    """Admin: show or hide the participant-facing leaderboard."""
    try:
        table.update_item(
            Key={'TeamID': 'SYSTEM_SETTINGS'},
            UpdateExpression='SET LeaderboardVisible = :val',
            ExpressionAttributeValues={':val': req.visible}
        )
        return {'message': 'Leaderboard visibility updated.', 'visible': req.visible}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.post("/settings/threshold")
def set_qualification_threshold(req: SetThresholdRequest):
    """Admin: set, update, publish or republish the score cutoff threshold."""
    if req.threshold < 1 or req.threshold > 200:
        raise HTTPException(status_code=400, detail="Threshold must be between 1 and 200.")
    try:
        table.update_item(
            Key={'TeamID': 'SYSTEM_SETTINGS'},
            UpdateExpression='SET ThresholdValue = :th, ThresholdPublished = :pub, ThresholdVisible = :vis',
            ExpressionAttributeValues={
                ':th': int(req.threshold),
                ':pub': bool(req.published),
                ':vis': bool(req.visible)
            }
        )
        return {
            'message': f'Threshold set to {req.threshold} successfully.',
            'threshold': req.threshold,
            'published': req.published,
            'visible': req.visible
        }
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.post("/settings/toggle-threshold")
def toggle_threshold_visibility(req: ToggleThresholdRequest):
    """Admin: toggle whether the threshold shortlisted teams are shown to users."""
    try:
        table.update_item(
            Key={'TeamID': 'SYSTEM_SETTINGS'},
            UpdateExpression='SET ThresholdVisible = :vis',
            ExpressionAttributeValues={':vis': bool(req.visible)}
        )
        return {'message': 'Threshold visibility updated.', 'visible': req.visible}
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.get("/threshold/public")
def get_public_threshold():
    """
    Participant-facing endpoint: returns the cutoff threshold and qualified teams
    only if threshold is published and visible.
    """
    try:
        settings_res = table.get_item(Key={'TeamID': 'SYSTEM_SETTINGS'})
        raw_settings = settings_res.get('Item', {})
        published = bool(raw_settings.get('ThresholdPublished', False))
        visible = bool(raw_settings.get('ThresholdVisible', False))
        threshold_val = int(raw_settings.get('ThresholdValue', 0))

        if not (published and visible and threshold_val > 0):
            return {
                'published': published,
                'visible': visible,
                'threshold': threshold_val,
                'count': 0,
                'qualified_teams': []
            }

        full_data = compute_leaderboard_payload(raw_settings, True)
        overall = full_data.get('overall', [])
        qualified = [t for t in overall if int(t.get('score', 0)) >= threshold_val]

        return {
            'published': published,
            'visible': visible,
            'threshold': threshold_val,
            'count': len(qualified),
            'qualified_teams': qualified
        }
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


def compute_leaderboard_payload(settings: dict, leaderboard_visible: bool):
    team_reviews = settings.get('TeamReviewScores', {})
    legacy_jury_scores = settings.get('JuryScores', {})

    # Fetch all registered teams
    teams_res = table.scan()
    teams = [
        t for t in teams_res.get('Items', [])
        if t.get('TeamID') not in ('SYSTEM_SETTINGS',)
    ]
    team_map = {t['TeamID']: t for t in teams}

    all_entries = []
    # Only process registered teams that exist in the system
    for tid, t in team_map.items():
        trec = team_reviews.get(tid)
        matching_legacy = [
            v for k, v in legacy_jury_scores.items() 
            if (k.startswith(f"jury1_{tid}") or k.startswith(f"jury2_{tid}") or k.startswith(f"jury3_{tid}") 
                or f"_{tid}_" in k or k.endswith(f"_{tid}"))
        ]
        
        # If no evaluations exist for this team, skip from leaderboard
        if not trec and not matching_legacy:
            continue

        r1_totals = []
        r2_totals = []
        jurors_set = set()

        if trec and isinstance(trec, dict):
            r1 = trec.get('review1')
            r2 = trec.get('review2')
            if r1 and isinstance(r1, dict) and r1.get('total') is not None:
                r1_totals.append(float(r1['total']))
                if r1.get('juror_id'):
                    jurors_set.add(r1['juror_id'])
            if r2 and isinstance(r2, dict) and r2.get('total') is not None:
                r2_totals.append(float(r2['total']))
                if r2.get('juror_id'):
                    jurors_set.add(r2['juror_id'])

        for leg in matching_legacy:
            if isinstance(leg, dict):
                tot = leg.get('total')
                if tot is not None:
                    rnd = leg.get('review_round', 'review1')
                    if rnd == 'review2':
                        r2_totals.append(float(tot))
                    else:
                        r1_totals.append(float(tot))
                    if leg.get('juror_id'):
                        jurors_set.add(leg['juror_id'])

        # Compute average score per round (so multiple jurors are averaged out of 100, not summed)
        r1_avg = round(sum(r1_totals) / len(r1_totals), 1) if r1_totals else 0.0
        r2_avg = round(sum(r2_totals) / len(r2_totals), 1) if r2_totals else 0.0

        # Total score: Review 1 average + Review 2 average (out of 200)
        # If only Review 1 was evaluated, total is Review 1 average (out of 100)
        total_score = round(r1_avg + r2_avg, 1)

        # Count of distinct jurors who evaluated this team
        jury_count = max(len(jurors_set), (1 if r1_totals else 0) + (1 if r2_totals else 0))

        # Clean integer display if whole number (e.g. 91 instead of 91.0)
        score_val = int(total_score) if total_score == int(total_score) else total_score
        r1_val = int(r1_avg) if r1_avg == int(r1_avg) else r1_avg
        r2_val = int(r2_avg) if r2_avg == int(r2_avg) else r2_avg

        all_entries.append({
            'team_id':          tid,
            'team_name':        t.get('Team Name') or t.get('TeamName') or tid,
            'assigned_problem': t.get('AdminAssignedProblem') or t.get('SelectedProblem') or '',
            'assigned_title':   t.get('AdminAssignedProblemTitle') or t.get('SelectedProblem') or '',
            'sdg_id':           t.get('AdminAssignedSdgId') or t.get('SdgId') or 'GENERAL',
            'score':            score_val,
            'avg_score':        score_val,
            'r1_score':         r1_val,
            'r2_score':         r2_val,
            'jury_count':       jury_count,
        })

    overall = sorted(all_entries, key=lambda x: x['score'], reverse=True)

    for i, entry in enumerate(overall):
        entry['overall_rank'] = i + 1

    top3_overall_ids = {e['team_id'] for e in overall[:3]}

    SDG_IDS = ['SDG2', 'SDG3', 'SDG4', 'SDG6', 'SDG11', 'SDG13', 'HARDWARE']
    sdg_boards = {}
    for sdg_id in SDG_IDS:
        # Exclude teams present in overall leaderboard (Top 3 Overall) so the next team gets the rank
        sdg_entries = [e for e in overall if e.get('sdg_id') == sdg_id and e['team_id'] not in top3_overall_ids]
        for i, entry in enumerate(sdg_entries):
            entry_copy = dict(entry)
            entry_copy['sdg_rank'] = i + 1
            entry_copy['excluded_from_top3_display'] = False
            sdg_entries[i] = entry_copy

        sdg_boards[sdg_id] = sdg_entries

    return {
        'visible':   leaderboard_visible,
        'overall':   overall,
        'sdg':       sdg_boards,
        'by_sdg':    sdg_boards,
    }


@app.get("/leaderboard")
def get_leaderboard_public():
    """
    Computes and returns the participant-facing leaderboard.
    If LeaderboardVisible is False, returns empty scores so participants cannot view early results.
    """
    try:
        settings_res = table.get_item(Key={'TeamID': 'SYSTEM_SETTINGS'})
        settings = settings_res.get('Item', {})
        leaderboard_visible = bool(settings.get('LeaderboardVisible', False))
        if not leaderboard_visible:
            return {
                'visible': False,
                'overall': [],
                'sdg': {},
                'by_sdg': {},
                'message': 'Leaderboards are currently sealed by event administrators.'
            }
        return compute_leaderboard_payload(settings, True)
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


@app.get("/leaderboard/data")
def get_leaderboard_admin():
    """
    Computes and returns the complete leaderboard data for the Admin console.
    Always returns computed scores regardless of participant visibility.
    """
    try:
        settings_res = table.get_item(Key={'TeamID': 'SYSTEM_SETTINGS'})
        settings = settings_res.get('Item', {})
        leaderboard_visible = bool(settings.get('LeaderboardVisible', False))
        return compute_leaderboard_payload(settings, leaderboard_visible)
    except ClientError as e:
        raise HTTPException(status_code=500, detail=e.response['Error']['Message'])


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
