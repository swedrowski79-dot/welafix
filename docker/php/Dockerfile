FROM php:8.3-apache

RUN apt-get update && apt-get install -y     gnupg2 apt-transport-https ca-certificates     curl unzip git     sqlite3 libsqlite3-dev     libzip-dev zip  && rm -rf /var/lib/apt/lists/*

# PHP Extensions (sqlite/zip/etc.)
RUN docker-php-ext-install pdo pdo_sqlite zip

# Microsoft ODBC + sqlsrv
RUN curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/microsoft.gpg  && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list  && apt-get update  && ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev  && pecl install sqlsrv pdo_sqlsrv  && docker-php-ext-enable sqlsrv pdo_sqlsrv  && rm -rf /var/lib/apt/lists/*

# Apache
RUN a2enmod rewrite headers

# Apache vhost
COPY docker/php/apache.conf /etc/apache2/sites-available/000-default.conf

# Composer (optional; uncomment if you want it)
# COPY --from=composer:2 /usr/bin/composer /usr/bin/composer

WORKDIR /var/www/html
