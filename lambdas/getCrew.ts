import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { CrewQueryParams } from "../shared/types"
import {
    DynamoDBDocumentClient,
    GetCommand,
    QueryCommand,
    QueryCommandInput,
} from "@aws-sdk/lib-dynamodb";
import Ajv from "ajv";
import schema from "../shared/types.schema.json";

const ajv = new Ajv();
const isValidQueryParams = ajv.compile(
    schema.definitions["CrewQueryParams"] || {}
);

const ddbDocClient = createDocumentClient();

export const handler: APIGatewayProxyHandlerV2 = async (event, context) => {
    try {

        console.log("Event: ", event);
        const parameters = event?.pathParameters;
        const crewRole = parameters?.crewRole
            ? parseInt(parameters.crewRole)
            : undefined;
        const movieId = parameters?.movieId
            ? parseInt(parameters.movieId)
            : undefined;
/*
        const getItemCommand = new GetCommand({
            TableName: process.env.PLAYER_STATS_TABLE,
            Key: { movieId: movieId, crewRole: crewRole },
        });
        const { Item } = await ddbDocClient.send(getItemCommand);
        if (!Item) {
            return {
                statusCode: 404,
                body: JSON.stringify({ message: "Item not found" }),
            };
        }

*/



        const queryParams = event.queryStringParameters;
        if (queryParams && !isValidQueryParams(queryParams)) {
            return {
                statusCode: 500,
                headers: {
                    "content-type": "application/json",
                },
                body: JSON.stringify({
                    message: `Incorrect type. Must match Query parameters schema`,
                    schema: schema.definitions["CrewQueryParams"],
                }),
            };
        }

        let commandInput: QueryCommandInput = {
            TableName: process.env.TABLE_NAME,
        };

        if (queryParams) {
            if ("name" in queryParams) {
                commandInput = {
                    ...commandInput,
                    KeyConditionExpression: "movieId = :m and crewRole = :n and begins_with(roleName, :r) ",
                    ExpressionAttributeValues: {
                        ":n": crewRole,
                        ":m": movieId,
                        ":r": queryParams.roleName,
                    },
                };
            }

        }
        else {
            commandInput = {
                ...commandInput,
                KeyConditionExpression: "crewRole = :r and movieId = :m",
                ExpressionAttributeValues: {
                    ":r": crewRole,
                    ":m": movieId,
                },
            }
        }
        const commandOutput = await ddbDocClient.send(
            new QueryCommand(commandInput)
        );


        return {
            statusCode: 200,
            headers: {
                "content-type": "application/json",
            },
            body: JSON.stringify({
                data: commandOutput.Items,
            }),
        };
    } catch (error: any) {
        console.log(JSON.stringify(error));
        return {
            statusCode: 500,
            headers: {
                "content-type": "application/json",
            },
            body: JSON.stringify({ error }),
        };
    }
};

function createDocumentClient() {
    const ddbClient = new DynamoDBClient({ region: process.env.REGION });
    const marshallOptions = {
        convertEmptyValues: true,
        removeUndefinedValues: true,
        convertClassInstanceToMap: true,
    };
    const unmarshallOptions = {
        wrapNumbers: false,
    };
    const translateConfig = { marshallOptions, unmarshallOptions };
    return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}
