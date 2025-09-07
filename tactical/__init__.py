
import warnings

warnings.filterwarnings(
    "ignore",
    message="Protobuf gencode version .* is exactly one major version older.*",
    module="google.protobuf.runtime_version",
)
