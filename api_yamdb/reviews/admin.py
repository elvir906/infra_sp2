from django.contrib import admin

from reviews.models import Category, Comments, Genre, Review, Title


@admin.register(Title, Genre, Category, Review, Comments)
class TitGenCatRevCommAdmin(admin.ModelAdmin):
    search_filter = ('Title', 'category')
    search_fields = ('Title', 'category', 'genre')
