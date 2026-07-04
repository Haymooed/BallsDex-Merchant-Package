from django.db import migrations, models

class Migration(migrations.Migration):

    dependencies = [
        ('merchant', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='ActiveMerchant',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('guild_id', models.BigIntegerField()),
                ('channel_id', models.BigIntegerField()),
                ('message_id', models.BigIntegerField(unique=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
            ],
            options={
                'verbose_name': 'Active merchant',
            },
        ),
        migrations.AddField(
            model_name='merchantsettings',
            name='sale_percentage',
            field=models.PositiveSmallIntegerField(default=0, help_text='Global sale percentage (0-100). Increases the attractiveness of offers.'),
        ),
    ]
