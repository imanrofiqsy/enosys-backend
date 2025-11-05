from django.http import HttpResponseForbidden
import os

ALLOWED_PLC_IPS = [os.getenv("PLC_IP")]

class AllowOnlyPLC:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        forwarded = request.META.get('HTTP_X_FORWARDED_FOR')
        if forwarded:
            ip = forwarded.split(',')[0].strip()
        else:
            ip = request.META.get('REMOTE_ADDR')

        if ip not in ALLOWED_PLC_IPS:
            print(f"🚫 Access denied from {ip}")  # log tambahan
            return HttpResponseForbidden(f"Access denied: not authorized PLC IP. ip:{ip}")

        # print(f"✅ Access granted to {ip}")
        return self.get_response(request)
