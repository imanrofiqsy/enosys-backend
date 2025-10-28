from django.urls import path
from bms_0n import views

urlpatterns = [
    path('', views.dashboard, name='dashboard'),
]
