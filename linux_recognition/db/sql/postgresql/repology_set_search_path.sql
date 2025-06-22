ALTER ROLE current_user IN DATABASE {{dbname|identifier}} SET search_path TO {{schemas|map('identifier')|join(', ')}};
