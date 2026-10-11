/* AWS Amplify Configuration for ThermalMatch */
const awsmobile = {
    "aws_project_region": "us-east-1",
    "aws_cognito_region": "us-east-1",
    "aws_cognito_identity_pool_id": "us-east-1:78a1bc23-4567-89de-f012-3456789abcde",
    "aws_user_pools_id": "us-east-1_TMUserPool",
    "aws_user_pools_web_client_id": "7tm1234567890abcdefghijkl",
    "aws_cloud_logic_custom": [
        {
            "name": "ThermalMatchApi",
            "endpoint": window.location.origin,
            "region": "us-east-1"
        }
    ],
    "aws_kinesis_stream_name": "thermalmatch-exhaust-telemetry",
    "aws_kinesis_shards": 4,
    "aws_quicksight_dashboard_id": "tm-district-analytics-v2",
    "gemini_api_id": "SSM_MANAGED_GEMINI_KEY"
};

if (typeof module !== 'undefined') {
    module.exports = awsmobile;
}
