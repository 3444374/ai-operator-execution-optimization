"""Passive Linux process-resource observation (RSS, threads, classified FDs)."""
from .model import FdIdentity, FdKind, ProcessSnapshot, ResourceTrace

__all__ = ["FdIdentity", "FdKind", "ProcessSnapshot", "ResourceTrace"]
