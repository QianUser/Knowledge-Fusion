"""
Django settings for knowledge_fusion project.

Generated by 'django-admin startproject' using Django 3.1.7.

For more information on this file, see
https://docs.djangoproject.com/en/3.1/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/3.1/ref/settings/
"""
import logging
import os
import time
from pathlib import Path

# Build paths inside the project like this: BASE_DIR / 'subdir'.

BASE_DIR = Path(__file__).resolve().parent.parent


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/3.1/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'tj%%arhqofo)f!k9=chl)$r12+3r)f53jlws2b)km(n*&6x#%r'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = []


# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'knowledge_fusion.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'knowledge_fusion.wsgi.application'


# Database
# https://docs.djangoproject.com/en/3.1/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'db.sqlite3',
    }
}


# Password validation
# https://docs.djangoproject.com/en/3.1/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
# https://docs.djangoproject.com/en/3.1/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_L10N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/3.1/howto/static-files/

STATIC_URL = '/static/'

# 配置文件目录
config_dir = os.path.join(BASE_DIR, 'config.ini')

# # 使用北京时间
# logging.Formatter.converter = get_beijing_time

log_path = os.path.join(BASE_DIR, 'log')

if not os.path.exists(log_path):
    os.makedirs(log_path)

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {  # 格式化
        'simple': {
            'format': '[%(asctime)s] [%(module)s: %(funcName)s - line %(lineno)d] [%(levelname)s] - %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
        'console': {
            'format': '[%(asctime)s] [%(module)s: %(funcName)s - line %(lineno)d] [%(levelname)s] - %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        }
    },
    'handlers': {
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'console'
        },
        'apiHandler': {
            'level': 'INFO',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            "when": 'D',
            'formatter': 'simple',
            'filename': '%s/server.log' % log_path
        },
        'matchHandler': {
            'level': 'DEBUG',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            "when": 'D',
            'formatter': 'simple',
            'filename': '%s/match.log' % log_path
        },
        'databaseHandler': {
            'level': 'DEBUG',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            "when": 'D',
            'formatter': 'simple',
            'filename': '%s/database.log' % log_path
        }
    },
    'loggers': {
        'server_log': {
            'handlers': ['console', 'apiHandler'],
            'level': 'INFO',
            'propagate': False
        },
        # ! 这个log貌似没有用到
        'match_log': {
            'handlers': ['console', 'matchHandler'],
            'level': 'DEBUG',
            'propagate': False
        },
        'database_log': {
            'handlers': ['console', 'databaseHandler'],
            'level': 'DEBUG',
            'propagate': False
        }

    }
}
