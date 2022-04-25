from django.contrib.auth.models import AbstractUser
from django.db import models


class User(AbstractUser):
    USER = 'user'
    MODERATOR = 'moderator'
    ADMIN = 'admin'
    ROLES = [
        (USER, 'пользователь'),
        (MODERATOR, 'модератор'),
        (ADMIN, 'администратор'),
    ]
    username = models.CharField(max_length=150, unique=True)
    email = models.EmailField(max_length=254, unique=True)
    first_name = models.CharField(max_length=150, blank=True)
    last_name = models.CharField(max_length=150, blank=True)
    bio = models.TextField('Биография', blank=True)
    role = models.CharField(
        'Права',
        choices=ROLES,
        default=USER,
        max_length=max(len(role[0]) for role in ROLES)
    )

    class Meta:
        ordering = ('username',)

    def __str__(self):
        return (
            f'Ник: {self.username} | '
            f'Почта: {self.email} | '
            f'Имя: {self.first_name} | '
            f'Фамилия: {self.last_name} | '
            f'Права доступа: {self.role}. | '
            f'Биография: {self.bio[:15]}'
        )

    @property
    def is_admin(self):
        return (
            self.role == self.ADMIN
            or self.is_staff
        )

    @property
    def is_moder(self):
        return self.role == self.MODERATOR
