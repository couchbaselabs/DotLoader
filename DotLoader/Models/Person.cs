using FakerDotNet;

namespace NET_App.Models;

/**
 * Some random document type to use in doc loading
 * Copied from - https://github.com/couchbaselabs/sirius/blob/main/internal/template/template_person.go
 */
public class Person
{
    public string firstName  {get; set;}
    public string lastName {get; set;}
    public int age {get; set;}
    public string email {get; set;}
    public Address address { get; set; }
    public string gender { get; set; }
    public string maritalSttus { get; set; }
    public string[] hobbies { get; set; }
    public Attribute attributes { get; set; }
    public string payload { get; set; }
}

public class Address
{
    public string street  {get; set;}
    public string city {get; set;}
    public string state  {get; set;}
    public string zipcode {get; set;}
    public string country  {get; set;}
}

public class Hair
{
    public string type  {get; set;}
    public string colour {get; set;}
    public string length  {get; set;}
    public string thickness {get; set;}
}

public class Attribute
{
    public int weight  {get; set;}
    public int height {get; set;}
    public string colour  {get; set;}
    public Hair hair {get; set;}
    public string bodyType  {get; set;}

}

public class PersonGenerator
{
    private static string[] maritalChoices = {"Single", "Married", "Divorcee"};
    private static string[] bodyColor = {"Dark", "Fair", "Brown", "Grey"};

    private static string[] hobbyChoices =
    {
        "Video Gaming", "Football", "Basketball", "Cricket",
        "Hockey", "Running", "Walking", "Guitar", "Flute", "Piano", "Chess", "Puzzle", "Skating", "Travelling"
    };

    private static string[] HairType = {"straight", "wavy", "curly", "Coily"};
    private static string[] HairColor = {"Red", "Green", "Yellow", "Grey", "Brown", "Black"};
    private static string[] HairLength = {"Long", "Short", "Medium"};
    private static string[] HairThickness = {"Thick", "Thin", "Medium"};

    private static string[] BodyType =
    {
        "Ectomorph", "endomorph", "Mesomorph", "triangle", "Inverted triangle",
        "Rectangle", "Hourglass", "apple"
    };

    private static string[] genders = {
        "male", "female", "other"
    };

    public static Person GenerateDocument(int docSize)
    {
        var rand = new Random();
        var person = new Person()
        {
            firstName = Faker.Name.FirstName(),
            lastName = Faker.Name.LastName(),
            age = rand.Next(0, 100),
            email = Faker.Internet.Email(),
            gender = genders[rand.Next(0, genders.Length -1)],
            maritalSttus = maritalChoices[rand.Next(0, maritalChoices.Length -1)],
            hobbies = hobbyChoices,
            address = new Address()
            {
                state = Faker.Address.State(),
                city = Faker.Address.City(),
                street = Faker.Address.StreetAddress(),
                zipcode = Faker.Address.ZipCode(),
                country = Faker.Address.Country()
            },
            attributes = new Attribute()
            {
                weight = rand.Next(55, 200),
                height = rand.Next(100,300),
                colour = bodyColor[rand.Next(0, bodyColor.Length - 1)],
                hair = new Hair()
                {
                    type = HairType[rand.Next(0, HairType.Length -1)],
                    colour = HairColor[rand.Next(0, HairColor.Length -1)],
                    length = HairLength[rand.Next(0, HairLength.Length -1)],
                    thickness = HairThickness[rand.Next(0, HairThickness.Length -1)]
                },
                bodyType = BodyType[rand.Next(0, BodyType.Length -1)]
            }
            
        };

        if (person.ToString().Length >= docSize) return person;
        var toAdd = docSize - person.ToString().Length;
        var randomString = Faker.LordOfTheRings.Character();
        while (randomString.Length < toAdd)
        {
            randomString += randomString;
        }
        person.payload = randomString;

        return person;
    }
}


