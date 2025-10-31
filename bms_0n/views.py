from django.http import HttpResponse, JsonResponse
import json

def dashboard(request):
    return JsonResponse({"status": "ok", "message": "Backend Railway is running"})

def test(request):
    if request.method == "POST":
        data = json.loads(request.body)
        print("data: ",data)