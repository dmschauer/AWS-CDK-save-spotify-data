#!/usr/bin/env python3

import aws_cdk as cdk

from save_spotify_data_cdk.save_spotify_data_cdk_stack import SaveSpotifyDataCdkStack


app = cdk.App()
SaveSpotifyDataCdkStack(app, "save-spotify-data-cdk")

app.synth()
