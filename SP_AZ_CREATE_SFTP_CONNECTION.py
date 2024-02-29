CREATE OR REPLACE PROCEDURE "SP_AZ_CREATE_SFTP_CONNECTION"("HOST" VARCHAR(16777216), "PORT" VARCHAR(16777216), "USERNAME" VARCHAR(16777216), "PASSWORD" VARCHAR(16777216), "SECRET_NAME" VARCHAR(16777216), "NETWORK_RULE_DB" VARCHAR(16777216), "NETWORK_RULE_SCHEMA" VARCHAR(16777216), "NETWORK_RULE_NAME" VARCHAR(16777216), "INTEGRATION_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

def main(session, host: str, port: str, username: str, password: str, secret_name: str, network_rule_db: str, network_rule_schema: str, network_rule_name: str, integration_name: str):
    
    secret_db = ''CPH_DB_COMMONS_DEV'' #liquibase parameter goes here
    secret_schema = ''COMMONS'' #liquibase parameter goes here
    
    create_statement_secret = f"""CREATE SECRET {secret_db}.{secret_schema}.{secret_name}
                                    TYPE = PASSWORD
                                    USERNAME = ''{username}''
                                    PASSWORD = ''{password}''
                                    """
    create_statement_network_rule = f"""CREATE NETWORK RULE {network_rule_db}.{network_rule_schema}.{network_rule_name}
                                            MODE = EGRESS
                                            TYPE = HOST_PORT
                                            VALUE_LIST = (''{host}:{port}'')
                                            """
    create_statement_access_integration = f"""CREATE EXTERNAL ACCESS INTEGRATION {integration_name}
                                                    ALLOWED_NETWORK_RULES = ({network_rule_db}.{network_rule_schema}.{network_rule_name})
                                                    ALLOWED_AUTHENTICATION_SECRETS = ({secret_db}.{secret_schema}.{secret_name})
                                                    ENABLED = True
                                                    """

    secret_result = ''''
    nr_result = ''''
    integration_result = ''''
    try: 
        secret_result = session.sql(create_statement_secret).collect()[0][0]
        nr_result = session.sql(create_statement_network_rule).collect()[0][0]
        integration_result = session.sql(create_statement_access_integration).collect()[0][0]
    
        return f"""Secret: ''{secret_result}'' \\nNetwork rule: ''{nr_result}'' \\nExternal access integration: ''{integration_result}''"""
    except Exception as e:
        raise Exception(f"""Creating SFTP connection failed with: ''{e.message}'' \\nSecret: ''{secret_result}'' \\nNetwork rule: ''{nr_result}'' \\nExternal access integration: ''{integration_result}''""") from e
';