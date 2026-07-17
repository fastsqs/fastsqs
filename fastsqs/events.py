import re
from datetime import datetime
from typing import Generic, Optional, Set, TypeVar

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

DataT = TypeVar("DataT")


class SQSEvent(BaseModel):
    """Base class for SQS event models.

    Subclasses declare typed fields; the class name is the message type used for
    routing, unless ``__message_type__`` is set on the class — that decouples the
    route key from the class name (e.g. a namespaced/versioned type such as
    ``"com.acme.payment.approved.v1"``). The override is own-class only: a
    subclass without its own ``__message_type__`` falls back to name derivation,
    so reusing a model via inheritance can never silently collide on the
    parent's key. Both snake_case field names and their camelCase aliases are
    accepted (Pydantic alias generation with ``populate_by_name``), so a payload
    may use either convention without bespoke normalization.
    """

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    @classmethod
    def get_message_type(cls) -> str:
        """Primary message type for this event class: the class's own
        ``__message_type__`` if set, else snake_case of the class name."""
        override = cls.__dict__.get("__message_type__")
        if override is not None:
            return override
        name = cls.__name__
        return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()

    @classmethod
    def get_message_type_variants(cls) -> Set[str]:
        """Message-type variants for flexible matching: the class name plus its
        snake_case, camelCase and kebab-case forms. With ``__message_type__``
        set, only the exact override — case/format variants make no sense for a
        namespaced/versioned type."""
        override = cls.__dict__.get("__message_type__")
        if override is not None:
            return {override}
        base_name = cls.__name__
        if not base_name:
            return set()
        snake = re.sub(r'(?<!^)(?=[A-Z])', '_', base_name).lower()
        camel = base_name[0].lower() + base_name[1:]
        kebab = re.sub(r'(?<!^)(?=[A-Z])', '-', base_name).lower()
        return {base_name, snake, camel, kebab}


class CloudEvent(SQSEvent, Generic[DataT]):
    """CloudEvents 1.0 structured-mode envelope with typed ``data``.

    Opt-in base for consuming (and producing) messages that follow the CNCF
    CloudEvents JSON format: the four required attributes (``specversion``,
    ``id``, ``source``, ``type``), the optional ones, and the domain payload
    under ``data`` — typed via the generic parameter. Spec *extension*
    attributes (e.g. ``traceparent``) live at the top level, so unknown keys
    are kept (``extra="allow"``) instead of dropped.

    Route it like any event model; set ``__message_type__`` to the event's
    ``type`` value so the (reverse-DNS, versioned) type is the route key::

        class PaymentApproved(CloudEvent[PaymentData]):
            __message_type__ = "com.acme.payment.approved.v1"
    """

    model_config = ConfigDict(extra="allow")

    specversion: str = "1.0"
    id: str
    source: str
    type: str
    time: Optional[datetime] = None
    subject: Optional[str] = None
    datacontenttype: Optional[str] = None
    dataschema: Optional[str] = None
    data: DataT
