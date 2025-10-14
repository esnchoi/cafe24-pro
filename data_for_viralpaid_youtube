name: Google Analytics Viral YouTube Weekly Update

on:
  schedule:
    # 매주 일요일 밤 11시 50분 (한국시간 기준, UTC로는 일요일 14:50)
    - cron: '50 14 * * 0'
  workflow_dispatch:  # 수동 실행도 가능하게

jobs:
  update-viral-youtube:
    runs-on: ubuntu-latest
    
    steps:
    - name: Checkout repository
      uses: actions/checkout@v3
    
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.10'
    
    - name: Install dependencies
      run: |
        pip install --upgrade pip
        pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client
    
    - name: Create client_secret.json
      run: |
        echo '${{ secrets.GA_CLIENT_SECRET_JSON }}' > client_secret.json
    
    - name: Create ga_token.json
      run: |
        echo '${{ secrets.GA_TOKEN_JSON }}' > ga_token.json
    
    - name: Run Viral YouTube Analytics Script
      env:
        GITHUB_ACTIONS: true
      run: |
        python GA_cafe24pro_data_for_viralpaid_youtube.py
