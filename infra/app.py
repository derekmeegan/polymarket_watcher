#!/usr/bin/env python3
import os
import aws_cdk as cdk
from stack import PolyMarketStack

app: cdk.App = cdk.App()
PolyMarketStack(
    app, 
    "PolyMarketStack",
    env=cdk.Environment(
        account=os.getenv("CDK_DEFAULT_ACCOUNT"),
        region="us-east-1"  # or your desired region
    )
)
app.synth()
