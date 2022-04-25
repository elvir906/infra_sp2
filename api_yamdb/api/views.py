from django.contrib.auth.tokens import default_token_generator
from django.core.exceptions import ObjectDoesNotExist
from django.core.mail import send_mail
from django.db.models.aggregates import Avg
from django.shortcuts import get_object_or_404

from rest_framework import filters, mixins, status, viewsets
from rest_framework.decorators import action, api_view
from rest_framework.filters import SearchFilter
from rest_framework.pagination import PageNumberPagination
from rest_framework.permissions import IsAuthenticated, SAFE_METHODS
from rest_framework.response import Response

from rest_framework_simplejwt.tokens import AccessToken

from api_yamdb.settings import EMAIL_DEFAULT
from api.filters import TitleFilter
from api.permissions import (
    AdminOnly,
    ReadOnly,
    IsAuthorOrModeratorOrAdminOrReadOnly
)
from api.serializers import (
    CategorySerializer,
    CommentsSerializer,
    GenreSerializer,
    ReviewsSerializer,
    SignUpSerializer,
    TitleCreateSerializer,
    TitleSerializer,
    TokenObtainSerializer,
    UserSerializer
)
from reviews.models import Category, Genre, Review, Title
from users.models import User


def get_token_for_user(user):
    return {'token': str(AccessToken.for_user(user))}


class CreateDestroyListViewSet(
    mixins.CreateModelMixin,
    mixins.ListModelMixin,
    mixins.DestroyModelMixin,
    viewsets.GenericViewSet
):
    permission_classes = (AdminOnly | ReadOnly,)
    filter_backends = (filters.SearchFilter,)
    search_fields = ('name',)
    lookup_field = 'slug'
    pagination_class = PageNumberPagination


class ReviewsViewSet(viewsets.ModelViewSet):
    serializer_class = ReviewsSerializer
    permission_classes = (IsAuthorOrModeratorOrAdminOrReadOnly,)
    pagination_class = PageNumberPagination

    def get_queryset(self):
        title = get_object_or_404(Title, id=self.kwargs.get('title_id'))
        return title.reviews.all()

    def perform_create(self, serializer):
        title = get_object_or_404(Title, id=self.kwargs.get('title_id'))
        serializer.save(author=self.request.user, title=title)


class CommentsViewSet(viewsets.ModelViewSet):
    serializer_class = CommentsSerializer
    permission_classes = (
        IsAuthorOrModeratorOrAdminOrReadOnly,
    )
    pagination_class = PageNumberPagination

    def get_queryset(self):
        review = get_object_or_404(Review, id=self.kwargs.get('review_id'))
        return review.comments.all()

    def perform_create(self, serializer):
        review = get_object_or_404(Review, id=self.kwargs.get('review_id'))
        serializer.save(author=self.request.user, review=review)


class TitleViewSet(viewsets.ModelViewSet):
    queryset = Title.objects.annotate(rating=Avg('reviews__score'))
    serializer_class = TitleSerializer
    permission_classes = (AdminOnly | ReadOnly,)
    filterset_class = TitleFilter
    pagination_class = PageNumberPagination

    def get_serializer_class(self):
        if self.request.method not in SAFE_METHODS:
            return TitleCreateSerializer
        return TitleSerializer


class GenreViewSet(CreateDestroyListViewSet):
    queryset = Genre.objects.all()
    serializer_class = GenreSerializer


class CategoryViewSet(CreateDestroyListViewSet):
    queryset = Category.objects.all()
    serializer_class = CategorySerializer


class UserViewSet(viewsets.ModelViewSet):
    serializer_class = UserSerializer
    queryset = User.objects.all()
    permission_classes = (AdminOnly,)
    pagination_class = PageNumberPagination
    filter_backends = (SearchFilter,)
    search_fields = ('username',)
    lookup_field = 'username'

    @action(
        methods=['get', 'patch'],
        detail=False,  # detail=True для URL "users/<pk>/", а здесь "users/me"
        url_path='me',
        permission_classes=[IsAuthenticated]
    )
    def current_user_data(self, request):
        if request.method == 'GET':
            serializer = self.get_serializer(request.user, many=False)
        else:
            serializer = self.get_serializer(
                request.user, data=request.data, partial=True
            )
            serializer.is_valid(raise_exception=True)
            serializer.save(role=request.user.role)
        return Response(serializer.data, status.HTTP_200_OK)


@api_view(['POST'])
def signup(request):
    serializer = SignUpSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)
    try:
        user = User.objects.get(**serializer.validated_data)
    except ObjectDoesNotExist:
        if (
           not User.objects.filter(
               username=serializer.validated_data['username']).exists()
           and not User.objects.filter(
               email=serializer.validated_data['email']).exists()
           ):
            user = User.objects.create_user(**serializer.validated_data)
        else:
            return Response(serializer.data, status.HTTP_400_BAD_REQUEST)
    confirmation_code = default_token_generator.make_token(user)
    send_mail(
        subject='Confirmation code.',
        message=f'Ваш код для получения токена: {confirmation_code}',
        from_email=EMAIL_DEFAULT,
        recipient_list=[serializer.validated_data['email']]
    )
    return Response(serializer.data, status.HTTP_200_OK)


@api_view(['POST'])
def token_obtain(request):
    serializer = TokenObtainSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)
    user = get_object_or_404(
        User, username=serializer.validated_data['username']
    )
    if default_token_generator.check_token(
        user, serializer.validated_data['confirmation_code']
    ):
        return Response(get_token_for_user(user), status.HTTP_200_OK)
    return Response(serializer.errors, status.HTTP_400_BAD_REQUEST)
