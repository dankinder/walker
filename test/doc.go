/*
Package test contains test code for the walker core

Note that these tests do depend on items (like mocks) from the helpers package,
so these tests must be in their own package and not in the base `walker`
package or else there would be a circular dependency. The alternative would be
to put all the helpers/mocks into the base walker package but having the tests
split out has other advantages anyway.
*/
package test
