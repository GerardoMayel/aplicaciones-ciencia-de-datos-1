# dashboard_project/urls.py
from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('model_monitor.urls')),  # Ruta raíz para el dashboard
    path('django_plotly_dash/', include('django_plotly_dash.urls')),
]