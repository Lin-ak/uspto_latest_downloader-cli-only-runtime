#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class PublicFileModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    file_name: str
    official_data_date: str = ""
    release_date_raw: str = ""
    file_size_bytes: int = 0
    download_url: str = ""
    downloaded_at: str = ""
    status: str = "available"


class PublicStateErrorModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    code: str
    message: str
    hint: str | None = None
    at: str | None = None
    retryable: bool = False


class PublicStatusModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    running: bool = False
    last_checked_at: str | None = None
    last_action: str | None = None
    latest_remote: PublicFileModel | None = None
    last_download: PublicFileModel | None = None
    last_error: PublicStateErrorModel | None = None
    last_success_at: str | None = None
    last_success_age_seconds: int | None = None
    last_success_outcome: str | None = None
    last_run_summary: str | None = None
    consecutive_failure_count: int = 0
    failure_cooldown_until: str | None = None
    failure_cooldown_remaining_seconds: int | None = None


class ApiErrorModel(BaseModel):
    code: str
    message: str
    hint: str | None = None
    details: dict[str, Any] | None = None


class ErrorResponseModel(BaseModel):
    ok: Literal[False] = False
    error: ApiErrorModel


class PublicStatusResponseModel(BaseModel):
    ok: Literal[True] = True
    data: PublicStatusModel


class HealthStatusModel(BaseModel):
    status: str
    service: str
    checks: dict[str, str] = Field(default_factory=dict)


class HealthResponseModel(BaseModel):
    ok: Literal[True] = True
    data: HealthStatusModel
