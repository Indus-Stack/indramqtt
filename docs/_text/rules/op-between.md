`BETWEEN` keeps a row when a value falls inside a closed range.

Both ends count: `temperature BETWEEN 60 AND 80` matches 60 and 80.
Values outside the range do not match. `NOT BETWEEN` negates the test.

The range works on numbers and on text. Both ends must be present;
a missing end is a parse error.
