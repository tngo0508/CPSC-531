#!/usr/bin/env python3
# THOMAS NGO

import pprint


from Chinook_Python import *
from collections import namedtuple


def helper(relation, fields, tuple_format):
    """build a new relation"""
    if not relation or not fields or not tuple_format:
        raise ValueError('arguments cannot be None')
    res = set()
    for tuple in relation:
        row = []
        for field in fields:
            row.append(getattr(tuple, field))
        res.add(tuple_format(*row))
    return res


def project(relation, columns):
    if not relation or not columns:
        raise ValueError('arguments cannot be None')

    fields = next(iter(relation))._fields
    if not all(col in fields for col in columns):
        raise ValueError('Cannot find columns: ', columns)
    Result = namedtuple('Result', columns)
    return helper(relation, columns, Result)


def select(relation, predicate):
    if not relation or not predicate:
        raise ValueError('arguments cannot be None')
    return set(tuple for tuple in relation if predicate(tuple))


def rename(relation, new_columns=None, new_relation=None):
    if not relation:
        raise ValueError('relation cannot be None')

    if not new_columns and not new_relation:
        return relation

    fields = next(iter(relation))._fields

    if new_columns and new_relation:
        new_relation_name = new_relation[0]
        tuple_name = type(next(iter(relation))).__name__
        new_columns.extend(fields[len(new_columns):])
        Result = namedtuple(new_relation_name, new_columns)
    elif not new_relation:
        if len(new_columns) <= len(fields):
            tuple_name = type(next(iter(relation))).__name__
            new_columns.extend(fields[len(new_columns):])
            Result = namedtuple(tuple_name, new_columns)
        else:
            raise ValueError('Too many fields')
    elif not new_columns and len(new_relation) == 1:
        new_relation_name = new_relation[0]
        Result = namedtuple(new_relation_name, fields)

    return helper(relation, fields, Result)


def cross(relation1, relation2):
    fields = list(next(iter(relation1))._fields)
    fields.extend(next(iter(relation2))._fields)
    Result = namedtuple('Result', fields)
    res = set(Result(*x, *y) for x in relation1 for y in relation2)
    print('{} tuples in total'.format(len(res)))
    return res


def theta_join(relation1, relation2, predicate):
    fields = list(next(iter(relation1))._fields)
    fields.extend(next(iter(relation2))._fields)
    Result = namedtuple('Result', fields)
    res = set(Result(*x, *y)
              for x in relation1 for y in relation2 if predicate(x, y))
    print('{} tuples in total'.format(len(res)))
    return res


def natural_join(relation1, relation2):

    # From https://stackoverflow.com/questions/21519200/natural-join-implementation-python
    f1 = list(next(iter(relation1))._fields)
    f2 = list(next(iter(relation2))._fields)

    r1 = set(f1)
    r2 = set(f2)

    # Find the join condition (intersection) between relation1 and relation2.
    # Then, find the set which is in relation1, but not in relation2
    join_cond = r1 & r2
    diff = r2 - join_cond

    # Generate the fields name for the result's tuples
    fields = [x for x in f1]
    fields.extend([x for x in f2 if x in diff])
    Result = namedtuple('Result', fields)

    res = set()
    for t1 in relation1:
        for t2 in relation2:

            # Check if all values in the same columns are matched
            if all(getattr(t1, col) == getattr(t2, col) for col in list(join_cond)):

                # Get the values of namedtupled in relation1 first
                row = [*t1]

                # Get the values of namedtupled in relation1
                # Skip values in join condition
                row.extend(
                    [v for k, v in t2._asdict().items() if k not in join_cond])
                res.add(Result(*row))
    print('{} tuples in total'.format(len(res)))
    return res


# Professor's test cases
print(format('Queries', '*^100s'))
print('\nCombing \u03C3 and x to implement \u03B8-join\n')
pprint.pprint(
    project(
        select(
            select(
                cross(
                    Album,
                    rename(Artist, ['Id', 'Name'])
                ),
                lambda t: t.ArtistId == t.Id
            ),
            lambda t: t.Name == 'Red Hot Chili Peppers'
        ),
        ['Title']
    )
)

print('\nPerforming \u03C3 after \u03B8-join\n')
pprint.pprint(
    project(
        select(
            theta_join(
                Album,
                rename(Artist, ['Id', 'Name']),
                lambda t1, t2: t1.ArtistId == t2.Id
            ),
            lambda t: t.Name == 'Red Hot Chili Peppers'
        ),
        ['Title']
    )
)

print('\nPerforming \u03C3 before \u03B8-join\n')
pprint.pprint(
    project(
        theta_join(
            Album,
            rename(
                select(Artist, lambda t: t.Name == 'Red Hot Chili Peppers'),
                ['Id', 'Name']
            ),
            lambda t1, t2: t1.ArtistId == t2.Id
        ),
        ['Title']
    )
)

print('\nExtra credit: Natural Join\n')
pprint.pprint(
    project(
        natural_join(
            Album,
            select(Artist, lambda t: t.Name == 'Red Hot Chili Peppers')
        ),
        ['Title']
    )
)

# Perfromance Measurement - Question 1
# We observed that the order of relational algebra operators affected the
# processing performance
# the deeper the SELECT operator was put in the queries, the less
# total number of tuples was generated

# Performance Measurement - Question 2
print(format('The last query from Project 1', '*^100s'))
pprint.pprint(
    project(
        theta_join(
            Employee,
            rename(
                project(
                    theta_join(
                        Customer,
                        rename(
                            project(
                                theta_join(
                                    Invoice,
                                    rename(
                                        project(
                                            theta_join(
                                                InvoiceLine,
                                                rename(
                                                    project(
                                                        theta_join(
                                                            Track,
                                                            rename(
                                                                project(
                                                                    select(MediaType,
                                                                           lambda t: t.Name == 'Purchased AAC audio file'),
                                                                    ['MediaTypeId']
                                                                ),
                                                                ['Id']
                                                            ),
                                                            lambda t1, t2: t1.MediaTypeId == t2.Id
                                                        ),
                                                        ['TrackId']
                                                    ),
                                                    ['Id']
                                                ),
                                                lambda t1, t2: t1.TrackId == t2.Id
                                            ),
                                            ['InvoiceId']
                                        ),
                                        ['Id']
                                    ),
                                    lambda t1, t2: t1.InvoiceId == t2.Id
                                ),
                                ['CustomerId']
                            ),
                            ['Id']
                        ),
                        lambda t1, t2: t1.CustomerId == t2.Id
                    ),
                    ['SupportRepId']
                ),
                ['Id']
            ),
            lambda t1, t2: t1.EmployeeId == t2.Id
        ),
        ['LastName', 'FirstName']
    ),
)

# Custom test cases
print(format('Custom test cases', '*^100s'))
print('\nTest case 1\n')
pprint.pprint(
    project(
        theta_join(
            rename(Customer, ['CustomerId', 'CFirstName', 'CLastName', 'CCompany', 'CAddress', 'CCity',
                              'CState', 'CCountry', 'CPostalCode', 'CPhone', 'CFax', 'CEmail', 'CSupportRepId']),
            Employee,
            lambda t1, t2: t1.CSupportRepId == t2.EmployeeId
        ),
        ['CCompany', 'CFirstName', 'CLastName']
    )
)

print('\nTest case 2\n')
pprint.pprint(
    project(
        natural_join(
            Genre,
            project(
                natural_join(
                    Track,
                    project(
                        select(
                            natural_join(
                                Artist,
                                Album
                            ),
                            lambda t: t.Name == 'U2'
                        ),
                        ['AlbumId']
                    )
                ),
                ['GenreId']
            )
        ),
        ['Name']
    )
)
