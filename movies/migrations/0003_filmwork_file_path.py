# Generated by Django 3.2 on 2022-11-26 16:52

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('movies', '0002_auto_20221120_1253'),
    ]

    operations = [
        migrations.AddField(
            model_name='filmwork',
            name='file_path',
            field=models.FilePathField(null=True),
        ),
    ]
