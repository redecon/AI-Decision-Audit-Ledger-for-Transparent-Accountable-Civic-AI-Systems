from pydantic import BaseModel, Field
from typing import Optional, List

class ToolResponse(BaseModel):
    event_id: Optional[str] = None
    stream_id: Optional[str] = None
    version: Optional[int] = None   # new stream position
    message: str

class ErrorResponse(BaseModel):
    error_type: str
    message: str
    stream_id: Optional[str] = None
    expected_version: Optional[int] = None
    actual_version: Optional[int] = None
    suggested_action: Optional[str] = None

# Request models for each tool
class SubmitCaseRequest(BaseModel):
    case_id: str
    source: str   # web, SMS, NGO
    description: str
    location: Optional[str] = None
    correlation_id: Optional[str] = None

class StartAgentSessionRequest(BaseModel):
    agent_id: str
    session_id: str
    context_source: str
    model_version: str
    correlation_id: Optional[str] = None

class CategorizeCaseRequest(BaseModel):
    case_id: str
    agent_id: str
    session_id: str
    category: str
    confidence_score: float = Field(..., ge=0.0, le=1.0)
    input_data: dict
    model_version: str
    correlation_id: Optional[str] = None

class RecordUrgencyRequest(BaseModel):
    case_id: str
    agent_id: str
    session_id: str
    urgency_level: str   # LOW, MEDIUM, HIGH, CRITICAL
    confidence_score: float
    model_version: str
    correlation_id: Optional[str] = None

class PolicyCheckRequest(BaseModel):
    case_id: str
    rule_id: str
    regulation_version: Optional[str] = None
    correlation_id: Optional[str] = None

class PolicyRuleResultRequest(BaseModel):
    case_id: str
    rule_id: str
    result: bool   # True = passed, False = failed
    detail: Optional[str] = None
    correlation_id: Optional[str] = None

class GenerateRecommendationRequest(BaseModel):
    case_id: str
    recommendation: str
    confidence_score: float
    supporting_agents: List[dict]  # [{agent_id, session_id}]
    correlation_id: Optional[str] = None

class HumanReviewRequest(BaseModel):
    case_id: str
    reviewer_id: str
    decision: str   # approved, rejected, additional_review_needed
    override_reason: Optional[str] = None
    correlation_id: Optional[str] = None

class EscalateRequest(BaseModel):
    case_id: str
    target_authority: str
    reason: str
    correlation_id: Optional[str] = None

class PublishRequest(BaseModel):
    case_id: str
    publication_channel: str
    summary: str
    correlation_id: Optional[str] = None

class IntegrityCheckRequest(BaseModel):
    entity_type: str   # "case", "agent", "compliance", etc.
    entity_id: str

class PolicyCheckResultRequest(BaseModel):
    case_id: str
    rule_id: str
    passed: bool
    detail: Optional[str] = None
    correlation_id: Optional[str] = None
