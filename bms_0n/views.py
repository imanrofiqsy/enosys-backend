from django.http import HttpResponse, JsonResponse

def dashboard(request):
    return JsonResponse({"status": "ok", "message": "Backend Railway is running"})

