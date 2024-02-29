CREATE OR REPLACE PROCEDURE "SP_AZ_CREATE_SFTP_LOADING_PROCEDURE"("HOST" VARCHAR(16777216), "PORT" VARCHAR(16777216), "FILE_TYPES" ARRAY, "SFTP_PATH" VARCHAR(16777216), "STAGE_PATH" VARCHAR(16777216), "SECRET_NAME" VARCHAR(16777216), "INTEGRATION_NAME" VARCHAR(16777216), "PROCEDURE_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

def main(session, host: str, port: str, file_types: list, sftp_path: str, stage_path: str, secret_name: str, integration_name: str, procedure_name: str):
    commons_db = ''CPH_DB_COMMONS_DEV'' #liquibase parameter goes here
    commons_schema = ''COMMONS'' #liquibase parameter goes here
    return_msg = ''''
    grant_msg = ''''
    file_type_patterns = ''''
    for i in file_types:
        file_type_patterns += f''{i}$,''
    file_type_patterns = file_type_patterns[0:-1]
# Connection test
# Create persistent procedure to load file into stage
    try:
        return_msg = session.sql(f"""
        CREATE PROCEDURE {commons_db}.{commons_schema}.{procedure_name}(test_flag boolean)
        RETURNS VARCHAR(16777216)
        LANGUAGE PYTHON
        RUNTIME_VERSION = ''3.10''
        PACKAGES = (''snowflake-snowpark-python'',''paramiko'',''re2'')
        HANDLER = ''main''
        EXTERNAL_ACCESS_INTEGRATIONS = ({integration_name})
        SECRETS = (''cred''= {secret_name})
        EXECUTE AS OWNER
        AS ''
import _snowflake
# import pysftp
import paramiko
import re
import os
from snowflake.snowpark.files import SnowflakeFile
def main(session, test_flag: bool):
    sftp_cred = _snowflake.get_username_password(''''cred'''');
    sftp_host = ''''{host}''''
    sftp_port = {port}
    sftp_username = sftp_cred.username
    sftp_password = sftp_cred.password
    # cnopts = pysftp.CnOpts()
    # cnopts.hostkeys = None
    patterns = ''''{file_type_patterns}''''.split('''','''')
    try:       
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.WarningPolicy)
        ssh.connect(hostname=sftp_host, port=sftp_port, username=sftp_username, password=sftp_password, timeout=20)
        sftp = ssh.open_sftp()
        if ''''{sftp_path}'''' in sftp.listdir():
            sftp.chdir(''''{sftp_path}'''')
            rdir=sftp.listdir()
            ret=''''''''
            if test_flag:
                ret += f''''The following files have been found at the specified location: {{str(rdir)}}''''
                if sftp: sftp.close()
            else:
                if patterns != ['''''''']:
                    for file in (rdir):
                        if any(re.search(pattern, file) for pattern in patterns):
                            try:
                                sftp.get(file, f''''/tmp/{{file}}'''')
                                session.file.put(f''''/tmp/{{file}}'''', ''''{stage_path}'''', auto_compress=False, overwrite=True)
                                ret += f''''{{file}} successfully copied to {stage_path}. ''''
                                if sftp: sftp.close()
                            except Exception as e:
                                ret += f''''Copying {{file}} failed with {{e}} ''''
                                if sftp: sftp.close()
                else:
                    ret += f''''No file types have been specified.''''
                    if sftp: sftp.close()
        else:
            ret = ''''The specified location does not exist.''''
            if sftp: sftp.close()
        return ret
    except Exception as e:
        try:
            if sftp: sftp.close()
        except:
            pass
        finally:
            return f" Error with SFTP : {{e}}"
                            ''""").collect()[0][0]
        grant_msg = session.sql(f''GRANT OWNERSHIP ON PROCEDURE {commons_db}.{commons_schema}.{procedure_name}(BOOLEAN) TO ROLE SYSADMIN REVOKE CURRENT GRANTS'').collect()[0][0]
        return_msg += '' Grant to SYSADMIN: ''
        return_msg += grant_msg
        return return_msg
    except Exception as e:
        raise Exception(f''Creating loading procedure failed with: {e}'')
';