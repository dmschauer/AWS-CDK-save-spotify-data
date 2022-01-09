# In a real application I would use AWS KMS and store the secrets there. 
# As outlined here, it would have to be deployed manually if you want to 
# use a custom/pre-defined value in your secret:
# - https://pypi.org/project/aws-cdk.aws-secretsmanager/
# 
# This demo is meant to focus on the ETL/API/CDK implemantion part though,
# so I use this template file to keep it simple. Some alternatives and points
# to consider:
# - https://stackoverflow.com/questions/25501403/storing-the-secrets-passwords-in-a-separate-file
# - https://www.realpythonproject.com/3-ways-to-store-and-read-credentials-locally-in-python/
# 
# Simply replace the ### with your own custom values and rename this file to 
# spotify_settings.py before deployment 
CLIENT_ID='###'
CLIENT_SECRET='###'