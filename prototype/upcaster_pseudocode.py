def upcast_case_categorized_v1_to_v2(event_v1):
    return {
        "case_id": event_v1["case_id"],
        "category": event_v1["category"],
        "confidence_score": event_v1["confidence_score"],
        "model_version": "UNKNOWN",
        "model_provider": "UNKNOWN"
    }

def upcast_urgency_scored_v1_to_v2(event_v1):
    return {
        "case_id": event_v1["case_id"],
        "urgency_level": event_v1["urgency_level"],
        "confidence_score": event_v1["confidence_score"],
        "model_version": "UNKNOWN",
        "model_provider": "UNKNOWN"
    }
