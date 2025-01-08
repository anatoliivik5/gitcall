from typing import Dict, List, Optional
from pydantic import ConfigDict, BaseModel

class BeelineCallsServiceResultDTO(BaseModel):
    list_of_dict_30: List[Dict]
    list_id_call: List[int]
    result_df: Optional[List[Dict]] | None
    model_config = ConfigDict(arbitrary_types_allowed=True)


class CallComeDataDTO(BaseModel):
    user_id: int
    date_from: str
    date_to: str
    duration_call_minut: int
    phone_client: str
    companyId: int


class SendToTimelineCallsLinksDTO(BaseModel):
    domain: str
    links: list
    companyId: int
    message: str



