from django.shortcuts import render
from django.http import HttpResponse, JsonResponse

# def dashboard(request):
#     context = {
#         # KPI Values
#         'value_box1': '85.2',
#         'unit_box1': '%',
#         'value_box2': '42.8',
#         'unit_box2': '%',
#         'value_box3': '32.5',
#         'unit_box3': '%',
#         'value_box4': '12,450',
#         'unit_box4': 'USD/month',
#         'value_box5': '2,850',
#         'unit_box5': 'kg CO₂/day',
        
#         # Best Power Mode
#         'best_power_text': 'Best Power Mode',
        
#         # Alarm counts
#         'alarm_counts': {
#             'high': 2,
#             'medium': 5,
#             'low': 3,
#             'total': 10
#         }
#     }
#     return render(request, 'Dashboard.html', context)

# def dashboard_data(request):
#     """API endpoint for KPI updates"""
#     data = {
#         'value_box1': "100",  # Your calculation
#         'unit_box1': '%',
#         'value_box2': "100",
#         'unit_box2': '%',
#         'value_box3': "100",
#         'unit_box3': '%',
#         'value_box4': "100",
#         'unit_box4': 'USD/month',
#         'value_box5': "100",
#         'unit_box5': 'kg CO₂/day',
#     }
#     return JsonResponse(data)

# def get_chart_data(request):
#     """API endpoint for chart data"""
    
#     # Example data structure
#     data = {
#         'LT1': [
#             {'label': 'Mon', 'y': 120},
#             {'label': 'Tue', 'y': 110},
#             {'label': 'Wed', 'y': 130},
#             # ... more data
#         ],
#         'LT2': [...],
#         'LT3': [...],
#         'LIFT': [...],
#         'AC_ROOF': [...],
#         'MACHINE': [...],
#         'SOLAR_AND_PLN': [...],
#         'PLN_LAMP': [...],
        
#         'GREEN_ENERGY': [
#             {'label': 'Mon', 'y': 500},
#             # ... more data
#         ],
#         'MAIN_POWER': [
#             {'label': 'Mon', 'y': 300},
#             # ... more data
#         ],
        
#         'TOTAL_DAILY': [
#             {'label': '1 Jun', 'y': 2682},
#             {'label': '5 Jun', 'y': 3048},
#             # ... more data
#         ]
#     }
    
#     return JsonResponse(data)

def dashboard(request):
    return render(request, 'Dashboard.html')

