from django.http import HttpResponse, JsonResponse
import json
from django.views.decorators.csrf import csrf_exempt
import logging

def dashboard(request):
    return JsonResponse({"status": "ok", "message": "Backend Railway is running"})

@csrf_exempt
def test(request):
    if request.method == "POST":
        data = request.body.decode(errors='ignore')
        logging.info("data: ",data)
    forwarded = request.META.get('HTTP_X_FORWARDED_FOR')
    remote = request.META.get('REMOTE_ADDR')
    return JsonResponse({
        "X-Forwarded-For": forwarded,
        "REMOTE_ADDR": remote
    })